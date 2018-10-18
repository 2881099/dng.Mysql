using SafeObjectPool;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MySql.Data.MySqlClient {

	public class MySqlConnectionPool : ObjectPool<MySqlConnection> {

		internal Action availableHandler;
		internal Action unavailableHandler;

		public MySqlConnectionPool(string name, string connectionString, Action availableHandler, Action unavailableHandler) : base(null) {
			var policy = new MySqlConnectionPoolPolicy {
				_pool = this,
				Name = name
			};
			this.Policy = policy;
			policy.ConnectionString = connectionString;

			this.availableHandler = availableHandler;
			this.unavailableHandler = unavailableHandler;
		}

		public void Return(Object<MySqlConnection> obj, Exception exception, bool isRecreate = false) {
			if (exception != null && exception is MySqlException) {
				try { if (obj.Value.Ping() == false) obj.Value.Open(); } catch { base.SetUnavailable(exception); }
			}
			base.Return(obj, isRecreate);
		}
	}

	public class MySqlConnectionPoolPolicy : IPolicy<MySqlConnection> {

		internal MySqlConnectionPool _pool;
		public string Name { get; set; } = "MySQL MySqlConnection 对象池";
		public int PoolSize { get; set; } = 100;
		public TimeSpan SyncGetTimeout { get; set; } = TimeSpan.FromSeconds(10);
		public int AsyncGetCapacity { get; set; } = 10000;
		public bool IsThrowGetTimeoutException { get; set; } = true;
		public int CheckAvailableInterval { get; set; } = 5;

		private string _connectionString;
		public string ConnectionString {
			get => _connectionString;
			set {
				_connectionString = value ?? "";
				Match m = Regex.Match(_connectionString, @"Max\s*pool\s*size\s*=\s*(\d+)", RegexOptions.IgnoreCase);
				if (m.Success == false || int.TryParse(m.Groups[1].Value, out var poolsize) == false || poolsize <= 0) poolsize = 100;
				PoolSize = poolsize;

				var initConns = new Object<MySqlConnection>[poolsize];
				for (var a = 0; a < poolsize; a++) try { initConns[a] = _pool.Get(); } catch { }
				foreach (var conn in initConns) _pool.Return(conn);
			}
		}


		public bool OnCheckAvailable(Object<MySqlConnection> obj) {
			if (obj.Value.Ping() == false) obj.Value.Open();
			return obj.Value.Ping();
		}

		public MySqlConnection OnCreate() {
			var conn = new MySqlConnection(_connectionString);
			return conn;
		}

		public void OnDestroy(MySqlConnection obj) {
			if (obj.State != ConnectionState.Closed) obj.Close();
			obj.Dispose();
		}

		public void OnGet(Object<MySqlConnection> obj) {

			if (_pool.IsAvailable) {

				if (DateTime.Now.Subtract(obj.LastReturnTime).TotalSeconds > 60 && obj.Value.Ping() == false) {

					try {
						obj.Value.Open();
					} catch (Exception ex) {
						if (_pool.SetUnavailable(ex) == true)
							throw new Exception($"【{this.Name}】状态不可用，等待后台检查程序恢复方可使用。{ex.Message}");
					}
				}
			}
		}

		async public Task OnGetAsync(Object<MySqlConnection> obj) {

			if (_pool.IsAvailable) {

				if (DateTime.Now.Subtract(obj.LastReturnTime).TotalSeconds > 60 && obj.Value.Ping() == false) {

					try {
						await obj.Value.OpenAsync();
					} catch (Exception ex) {
						if (_pool.SetUnavailable(ex) == true)
							throw new Exception($"【{this.Name}】状态不可用，等待后台检查程序恢复方可使用。{ex.Message}");
					}
				}
			}
		}

		public void OnGetTimeout() {

		}

		public void OnReturn(Object<MySqlConnection> obj) {

		}

		public void OnAvailable() {
			_pool.availableHandler?.Invoke();
		}

		public void OnUnavailable() {
			_pool.unavailableHandler?.Invoke();
		}
	}
}
