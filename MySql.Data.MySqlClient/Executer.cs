using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

namespace MySql.Data.MySqlClient {
	public partial class Executer : IDisposable {

		public bool IsTracePerformance { get; set; } = string.Compare(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"), "Development", true) == 0;
		public int SlaveCheckAvailableInterval = 5;
		public ILogger Log { get; set; }
		public ConnectionPool MasterPool { get; } = new ConnectionPool();
		public List<ConnectionPool> SlavePools { get; } = new List<ConnectionPool>();
		private int slaveUnavailables = 0;
		private object slaveLock = new object();
		private Random slaveRandom = new Random();

		void LoggerException(ConnectionPool pool, MySqlCommand cmd, Exception e, DateTime dt, string logtxt) {
			var logPool = this.SlavePools.Any() == false ? "" : (pool == this.MasterPool ? "【主库】" : $"【从库{this.SlavePools.IndexOf(pool)}】");
			if (IsTracePerformance) {
				TimeSpan ts = DateTime.Now.Subtract(dt);
				if (e == null && ts.TotalMilliseconds > 100)
					Log.LogWarning($"{logPool}执行SQL语句耗时过长{ts.TotalMilliseconds}ms\r\n{cmd.CommandText}\r\n{logtxt}");
			}

			if (e == null) return;
			string log = $"{logPool}数据库出错（执行SQL）〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓\r\n{cmd.CommandText}\r\n";
			foreach (MySqlParameter parm in cmd.Parameters)
				log += parm.ParameterName.PadRight(20, ' ') + " = " + (parm.Value ?? "NULL") + "\r\n";

			log += e.Message;
			Log.LogError(log);

			RollbackTransaction();
			cmd.Parameters.Clear();
			throw e;
		}

		void CheckPoolAvailable(ConnectionPool pool, int interval) {
			new Thread(() => {
				while (pool.IsAvailable == false) {
					Thread.CurrentThread.Join(TimeSpan.FromSeconds(interval));
					try {
						var conn = pool.GetConnection();
						if (conn.SqlConnection.State == ConnectionState.Closed || conn.SqlConnection.Ping() == false) conn.SqlConnection.Open();
						pool.ReleaseConnection(conn);
						break;
					} catch { }
				}
				if (pool.IsAvailable == false) {
					lock (slaveLock) {
						if (pool.IsAvailable == false) {
							slaveUnavailables--;
							pool.IsAvailable = true;
						}
					}
				}
			}).Start();
		}

		/// <summary>
		/// 若使用【读写分离】，查询【从库】条件cmdText.StartsWith("SELECT ")，否则查询【主库】
		/// </summary>
		/// <param name="readerHander"></param>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		public void ExecuteReader(Action<MySqlDataReader> readerHander, CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			ConnectionPool pool = this.MasterPool;
			bool isSlave = false;

			//读写分离规则
			if (this.SlavePools.Any() && cmdText.StartsWith("SELECT ", StringComparison.CurrentCultureIgnoreCase)) {
				var availables = slaveUnavailables == 0 ?
					//查从库
					this.SlavePools : (
					//查主库
					slaveUnavailables == this.SlavePools.Count ? new List<ConnectionPool>() :
					//查从库可用
					this.SlavePools.Where(sp => sp.IsAvailable).ToList());
				if (availables.Any()) {
					isSlave = true;
					pool = availables.Count == 1 ? this.SlavePools[0] : availables[slaveRandom.Next(availables.Count)];
				}
			}

			var pc = PrepareCommand(pool, cmd, cmdType, cmdText, cmdParms, ref logtxt);
			if (IsTracePerformance) logtxt += $"PrepareCommand: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			Exception ex = null;
			try {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				if (isSlave) {
					//从库查询切换，恢复
					bool isSlaveFail = false;
					try {
						if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
					} catch {
						isSlaveFail = true;
					}
					if (isSlaveFail) {
						if (pc.tran == null) {
							if (IsTracePerformance) logtxt_dt = DateTime.Now;
							pool.ReleaseConnection(pc.conn);
							if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
						}
						LoggerException(pool, cmd, new Exception("连接失败，准备切换其他可用服务器，并启动定时恢复机制"), dt, logtxt);

						bool isCheckAvailable = false;
						if (pool.IsAvailable) {
							lock (slaveLock) {
								if (pool.IsAvailable) {
									slaveUnavailables++;
									pool.IsAvailable = false;
									isCheckAvailable = true;
								}
							}
						}

						if (isCheckAvailable) CheckPoolAvailable(pool, SlaveCheckAvailableInterval); //间隔多少秒检查服务器可用性
						ExecuteReader(readerHander, cmdType, cmdText, cmdParms);
						return;
					}
				} else {
					//主库查询
					if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
				}
				if (IsTracePerformance) {
					logtxt += $"Open: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
					logtxt_dt = DateTime.Now;
				}
				using (MySqlDataReader dr = cmd.ExecuteReader()) {
					if (IsTracePerformance) logtxt += $"ExecuteReader: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
					while (true) {
						if (IsTracePerformance) logtxt_dt = DateTime.Now;
						bool isread = dr.Read();
						if (IsTracePerformance) logtxt += $"	dr.Read: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
						if (isread == false) break;

						if (readerHander != null) {
							object[] values = null;
							if (IsTracePerformance) {
								logtxt_dt = DateTime.Now;
								values = new object[dr.FieldCount];
								dr.GetValues(values);
								logtxt += $"	dr.GetValues: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
								logtxt_dt = DateTime.Now;
							}
							readerHander(dr);
							if (IsTracePerformance) logtxt += $"	readerHander: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms ({string.Join(",", values)})\r\n";
						}
					}
					if (IsTracePerformance) logtxt_dt = DateTime.Now;
					dr.Close();
				}
				if (IsTracePerformance) logtxt += $"ExecuteReader_dispose: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (pc.tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				pool.ReleaseConnection(pc.conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(pool, cmd, ex, dt, logtxt);
		}
		/// <summary>
		/// 若使用【读写分离】，查询【从库】条件cmdText.StartsWith("SELECT ")，否则查询【主库】
		/// </summary>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		/// <returns></returns>
		public object[][] ExecuteArray(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			List<object[]> ret = new List<object[]>();
			ExecuteReader(dr => {
				object[] values = new object[dr.FieldCount];
				dr.GetValues(values);
				ret.Add(values);
			}, cmdType, cmdText, cmdParms);
			return ret.ToArray();
		}
		/// <summary>
		/// 在【主库】执行
		/// </summary>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		/// <returns></returns>
		public int ExecuteNonQuery(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pc = PrepareCommand(this.MasterPool, cmd, cmdType, cmdText, cmdParms, ref logtxt);
			int val = 0;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
				val = cmd.ExecuteNonQuery();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (pc.tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.MasterPool.ReleaseConnection(pc.conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(this.MasterPool, cmd, ex, dt, logtxt);
			cmd.Parameters.Clear();
			return val;
		}
		/// <summary>
		/// 在【主库】执行
		/// </summary>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		/// <returns></returns>
		public object ExecuteScalar(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pc = PrepareCommand(this.MasterPool, cmd, cmdType, cmdText, cmdParms, ref logtxt);
			object val = null;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
				val = cmd.ExecuteScalar();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (pc.tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.MasterPool.ReleaseConnection(pc.conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(this.MasterPool, cmd, ex, dt, logtxt);
			cmd.Parameters.Clear();
			return val;
		}

		private (Connection2 conn, MySqlTransaction tran) PrepareCommand(ConnectionPool pool, MySqlCommand cmd, CommandType cmdType, string cmdText, MySqlParameter[] cmdParms, ref string logtxt) {
			DateTime dt = DateTime.Now;
			cmd.CommandType = cmdType;
			cmd.CommandText = cmdText;

			if (cmdParms != null) {
				foreach (MySqlParameter parm in cmdParms) {
					if (parm == null) continue;
					if (parm.Value == null) parm.Value = DBNull.Value;
					cmd.Parameters.Add(parm);
				}
			}

			Connection2 conn = null;
			MySqlTransaction tran = CurrentThreadTransaction;
			if (IsTracePerformance) logtxt += $"	PrepareCommand_part1: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms cmdParms: {cmdParms.Length}\r\n";

			if (tran == null) {
				if (IsTracePerformance) dt = DateTime.Now;
				conn = pool.GetConnection();
				cmd.Connection = conn.SqlConnection;
				if (IsTracePerformance) logtxt += $"	PrepareCommand_tran==null: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			} else {
				if (IsTracePerformance) dt = DateTime.Now;
				cmd.Connection = tran.Connection;
				cmd.Transaction = tran;
				if (IsTracePerformance) logtxt += $"	PrepareCommand_tran!=null: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			}

			if (IsTracePerformance) dt = DateTime.Now;
			AutoCommitTransaction();
			if (IsTracePerformance) logtxt += $"	AutoCommitTransaction: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";

			return (conn, tran);
		}
	}
}
