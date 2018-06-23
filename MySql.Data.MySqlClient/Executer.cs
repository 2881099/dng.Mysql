using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;

namespace MySql.Data.MySqlClient {
	public partial class Executer : IDisposable {

		public bool IsTracePerformance { get; set; } = string.Compare(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"), "Development", true) == 0;
		public ILogger Log { get; set; }
		public ConnectionPool Pool { get; } = new ConnectionPool();

		void LoggerException(MySqlCommand cmd, Exception e, DateTime dt, string logtxt) {
			if (IsTracePerformance) {
				TimeSpan ts = DateTime.Now.Subtract(dt);
				if (e == null && ts.TotalMilliseconds > 100)
					Log.LogWarning($"执行SQL语句耗时过长{ts.TotalMilliseconds}ms\r\n{cmd.CommandText}\r\n{logtxt}");
			}

			if (e == null) return;
			string log = $"数据库出错（执行SQL）〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓\r\n{cmd.CommandText}\r\n";
			foreach (MySqlParameter parm in cmd.Parameters)
				log += parm.ParameterName.PadRight(20, ' ') + " = " + (parm.Value ?? "NULL") + "\r\n";

			log += e.Message;
			Log.LogError(log);

			RollbackTransaction();
			cmd.Parameters.Clear();
			throw e;
		}

		public void ExecuteReader(Action<MySqlDataReader> readerHander, CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pc = PrepareCommand(cmd, cmdType, cmdText, cmdParms, ref logtxt);
			if (IsTracePerformance) logtxt += $"PrepareCommand: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			Exception ex = null;
			try {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
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

			if (pc.Tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.Pool.ReleaseConnection(pc.Conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(cmd, ex, dt, logtxt);
		}
		public object[][] ExecuteArray(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			List<object[]> ret = new List<object[]>();
			ExecuteReader(dr => {
				object[] values = new object[dr.FieldCount];
				dr.GetValues(values);
				ret.Add(values);
			}, cmdType, cmdText, cmdParms);
			return ret.ToArray();
		}
		public int ExecuteNonQuery(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pc = PrepareCommand(cmd, cmdType, cmdText, cmdParms, ref logtxt);
			int val = 0;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
				val = cmd.ExecuteNonQuery();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (pc.Tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.Pool.ReleaseConnection(pc.Conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(cmd, ex, dt, logtxt);
			cmd.Parameters.Clear();
			return val;
		}
		public object ExecuteScalar(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			string logtxt = "";
			DateTime logtxt_dt = DateTime.Now;
			var pc = PrepareCommand(cmd, cmdType, cmdText, cmdParms, ref logtxt);
			object val = null;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) cmd.Connection.Open();
				val = cmd.ExecuteScalar();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (pc.Tran == null) {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				this.Pool.ReleaseConnection(pc.Conn);
				if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			}
			LoggerException(cmd, ex, dt, logtxt);
			cmd.Parameters.Clear();
			return val;
		}

		private PrepareCommandReturnInfo PrepareCommand(MySqlCommand cmd, CommandType cmdType, string cmdText, MySqlParameter[] cmdParms, ref string logtxt) {
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
				conn = this.Pool.GetConnection();
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

			return new PrepareCommandReturnInfo { Conn = conn, Tran = tran };
		}
		
		class PrepareCommandReturnInfo {
			public Connection2 Conn;
			public MySqlTransaction Tran;
		}
	}
}
