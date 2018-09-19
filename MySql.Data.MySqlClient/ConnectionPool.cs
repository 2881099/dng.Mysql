using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MySql.Data.MySqlClient {
	/// <summary>
	/// 数据库链接池
	/// </summary>
	public partial class ConnectionPool {

		private int _poolsize = 50;
		public List<Connection2> AllConnections = new List<Connection2>();
		public Queue<Connection2> FreeConnections = new Queue<Connection2>();
		public Queue<ManualResetEventSlim> GetConnectionQueue = new Queue<ManualResetEventSlim>();
		public Queue<TaskCompletionSource<Connection2>> GetConnectionAsyncQueue = new Queue<TaskCompletionSource<Connection2>>();
		private bool _isAvailable = true;
		public bool IsAvailable { get => _isAvailable; set { _isAvailable = value; UnavailableTime = value ? null : new DateTime?(DateTime.Now); } }
		public DateTime? UnavailableTime { get; set; }
		private static object _lock = new object();
		private static object _lock_GetConnectionQueue = new object();
		private string _connectionString;
		public string ConnectionString {
			get { return _connectionString; }
			set {
				_connectionString = value;
				if (string.IsNullOrEmpty(_connectionString)) return;
				Match m = Regex.Match(_connectionString, @"Max\s*pool\s*size\s*=\s*(\d+)", RegexOptions.IgnoreCase);
				if (m.Success) int.TryParse(m.Groups[1].Value, out _poolsize);
				else _poolsize = 50;
				if (_poolsize <= 0) _poolsize = 50;
				var initConns = new Connection2[_poolsize];
				for (var a = 0; a < _poolsize; a++) initConns[a] = GetFreeConnection();
				foreach (var conn in initConns) ReleaseConnection(conn);
			}
		}

		public Connection2 GetFreeConnection() {
			Connection2 conn = null;
			if (FreeConnections.Count > 0)
				lock (_lock)
					if (FreeConnections.Count > 0)
						conn = FreeConnections.Dequeue();
			if (conn == null && AllConnections.Count < _poolsize) {
				lock (_lock)
					if (AllConnections.Count < _poolsize) {
						conn = new Connection2();
						AllConnections.Add(conn);
					}
				if (conn != null) {
					conn.ThreadId = Thread.CurrentThread.ManagedThreadId;
					conn.SqlConnection = new MySqlConnection(ConnectionString);
				}
			}
			return conn;
		}
		public Connection2 GetConnection() {
			if (string.IsNullOrEmpty(ConnectionString)) throw new Exception("ConnectionString 未设置");
			var conn = GetFreeConnection();
			if (conn == null) {
				ManualResetEventSlim wait = new ManualResetEventSlim(false);
				lock (_lock_GetConnectionQueue)
					GetConnectionQueue.Enqueue(wait);
				if (wait.Wait(TimeSpan.FromSeconds(10)))
					return GetConnection();
				throw new Exception("MySql.Data.MySqlClient.ConnectionPool.GetConnection 连接池获取超时（10秒）");
			}
			conn.ThreadId = Thread.CurrentThread.ManagedThreadId;
			conn.LastActive = DateTime.Now;
			Interlocked.Increment(ref conn.UseSum);
			return conn;
		}

		async public Task<Connection2> GetConnectionAsync() {
			if (string.IsNullOrEmpty(ConnectionString)) throw new Exception("ConnectionString 未设置");
			var conn = GetFreeConnection();
			if (conn == null) {
				TaskCompletionSource<Connection2> tcs = new TaskCompletionSource<Connection2>();
				lock (_lock_GetConnectionQueue)
					GetConnectionAsyncQueue.Enqueue(tcs);
				return await tcs.Task;
			}
			conn.ThreadId = Thread.CurrentThread.ManagedThreadId;
			conn.LastActive = DateTime.Now;
			Interlocked.Increment(ref conn.UseSum);
			return conn;
		}

		public void ReleaseConnection(Connection2 conn) {
			try { conn.SqlConnection.Close(); } catch { }
			lock (_lock)
				FreeConnections.Enqueue(conn);

			bool isAsync = false;
			if (GetConnectionAsyncQueue.Count > 0) {
				TaskCompletionSource<Connection2> tcs = null;
				lock (_lock_GetConnectionQueue)
					if (GetConnectionAsyncQueue.Count > 0)
						tcs = GetConnectionAsyncQueue.Dequeue();
				if (isAsync = (tcs != null)) tcs.TrySetResult(GetConnectionAsync().Result);
			}
			if (isAsync == false && GetConnectionQueue.Count > 0) {
				ManualResetEventSlim wait = null;
				lock (_lock_GetConnectionQueue)
					if (GetConnectionQueue.Count > 0)
						wait = GetConnectionQueue.Dequeue();
				if (wait != null) wait.Set();
			}
		}
	}

	public class Connection2 {
		public MySqlConnection SqlConnection;
		public DateTime LastActive;
		public long UseSum;
		internal int ThreadId;
	}
}