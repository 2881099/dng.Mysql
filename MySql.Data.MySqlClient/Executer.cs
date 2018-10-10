using Microsoft.Extensions.Logging;
using SafeObjectPool;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

namespace MySql.Data.MySqlClient {
	public partial class Executer : IDisposable {

		public bool IsTracePerformance { get; set; } = string.Compare(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"), "Development", true) == 0;
		public ILogger Log { get; set; }
		public MySqlConnectionPool MasterPool { get; }
		public List<MySqlConnectionPool> SlavePools { get; } = new List<MySqlConnectionPool>();
		private int slaveUnavailables = 0;
		private object slaveLock = new object();
		private Random slaveRandom = new Random();

		void LoggerException(MySqlConnectionPool pool, MySqlCommand cmd, Exception e, DateTime dt, string logtxt, bool isThrowException = true) {
			if (IsTracePerformance) {
				TimeSpan ts = DateTime.Now.Subtract(dt);
				if (e == null && ts.TotalMilliseconds > 100)
					Log.LogWarning($"{pool.Policy.Name}执行SQL语句耗时过长{ts.TotalMilliseconds}ms\r\n{cmd.CommandText}\r\n{logtxt}");
			}

			if (e == null) return;
			string log = $"{pool.Policy.Name}数据库出错（执行SQL）〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓\r\n{cmd.CommandText}\r\n";
			foreach (MySqlParameter parm in cmd.Parameters)
				log += parm.ParameterName.PadRight(20, ' ') + " = " + (parm.Value ?? "NULL") + "\r\n";

			log += e.Message;
			Log.LogError(log);

			RollbackTransaction();
			cmd.Parameters.Clear();
			if (isThrowException) throw e;
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
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pool = this.MasterPool;
			bool isSlave = false;

			//读写分离规则
			if (this.SlavePools.Any() && cmdText.StartsWith("SELECT ", StringComparison.CurrentCultureIgnoreCase)) {
				var availables = slaveUnavailables == 0 ?
					//查从库
					this.SlavePools : (
					//查主库
					slaveUnavailables == this.SlavePools.Count ? new List<MySqlConnectionPool>() :
					//查从库可用
					this.SlavePools.Where(sp => sp.IsAvailable).ToList());
				if (availables.Any()) {
					isSlave = true;
					pool = availables.Count == 1 ? availables[0] : availables[slaveRandom.Next(availables.Count)];
				}
			}

			Object<MySqlConnection> conn = null;
			var pc = PrepareCommand(cmdType, cmdText, cmdParms, ref logtxt);
			if (IsTracePerformance) logtxt += $"PrepareCommand: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			Exception ex = null;
			try {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				if (isSlave) {
					//从库查询切换，恢复
					bool isSlaveFail = false;
					try {
						if (pc.cmd.Connection == null) pc.cmd.Connection = (conn = pool.Get()).Value;
						//if (slaveRandom.Next(100) % 2 == 0) throw new Exception("测试从库抛出异常");
					} catch {
						isSlaveFail = true;
					}
					if (isSlaveFail) {
						if (conn != null) {
							if (IsTracePerformance) logtxt_dt = DateTime.Now;
							pool.Return(conn, ex);
							if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
						}
						LoggerException(pool, pc.cmd, new Exception($"连接失败，准备切换其他可用服务器"), dt, logtxt, false);
						pc.cmd.Parameters.Clear();
						ExecuteReader(readerHander, cmdType, cmdText, cmdParms);
						return;
					}
				} else {
					//主库查询
					if (pc.cmd.Connection == null) pc.cmd.Connection = (conn = pool.Get()).Value;
				}
				if (IsTracePerformance) {
					logtxt += $"Open: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
					logtxt_dt = DateTime.Now;
				}
				using (MySqlDataReader dr = pc.cmd.ExecuteReader()) {
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

			if (conn != null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				pool.Return(conn, ex);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(pool, pc.cmd, ex, dt, logtxt);
			pc.cmd.Parameters.Clear();
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
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			Object<MySqlConnection> conn = null;
			var pc = PrepareCommand(cmdType, cmdText, cmdParms, ref logtxt);
			int val = 0;
			Exception ex = null;
			try {
				if (pc.cmd.Connection == null) pc.cmd.Connection = (conn = this.MasterPool.Get()).Value;
				val = pc.cmd.ExecuteNonQuery();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (conn != null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.MasterPool.Return(conn, ex);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(this.MasterPool, pc.cmd, ex, dt, logtxt);
			pc.cmd.Parameters.Clear();
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
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			Object<MySqlConnection> conn = null;
			var pc = PrepareCommand(cmdType, cmdText, cmdParms, ref logtxt);
			object val = null;
			Exception ex = null;
			try {
				if (pc.cmd.Connection == null) pc.cmd.Connection = (conn = this.MasterPool.Get()).Value;
				val = pc.cmd.ExecuteScalar();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (conn != null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.MasterPool.Return(conn, ex);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(this.MasterPool, pc.cmd, ex, dt, logtxt);
			pc.cmd.Parameters.Clear();
			return val;
		}

		private (MySqlTransaction tran, MySqlCommand cmd) PrepareCommand(CommandType cmdType, string cmdText, MySqlParameter[] cmdParms, ref string logtxt) {
			DateTime dt = DateTime.Now;
			var cmd = new MySqlCommand();
			cmd.CommandType = cmdType;
			cmd.CommandText = cmdText;

			if (cmdParms != null) {
				foreach (MySqlParameter parm in cmdParms) {
					if (parm == null) continue;
					if (parm.Value == null) parm.Value = DBNull.Value;
					cmd.Parameters.Add(parm);
				}
			}

			MySqlTransaction tran = CurrentThreadTransaction;
			if (IsTracePerformance) logtxt += $"	PrepareCommand_part1: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms cmdParms: {cmdParms.Length}\r\n";

			if (tran != null) {
				if (IsTracePerformance) dt = DateTime.Now;
				cmd.Connection = tran.Connection;
				cmd.Transaction = tran;
				if (IsTracePerformance) logtxt += $"	PrepareCommand_tran!=null: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			}

			if (IsTracePerformance) dt = DateTime.Now;
			AutoCommitTransaction();
			if (IsTracePerformance) logtxt += $"	AutoCommitTransaction: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";

			return (tran, cmd);
		}
	}
}
