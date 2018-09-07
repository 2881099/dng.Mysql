using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace MySql.Data.MySqlClient {
	partial class Executer {

		/// <summary>
		/// 若使用【读写分离】，查询【从库】条件cmdText.StartsWith("SELECT ")，否则查询【主库】
		/// </summary>
		/// <param name="readerHander"></param>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		/// <returns></returns>
		async public Task ExecuteReaderAsync(Func<MySqlDataReader, Task> readerHander, CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			DateTime logtxt_dt = DateTime.Now;
			ConnectionPool pool = this.MasterPool;
			//读写分离规则，暂时定为：所有查询的同步方法会读主库，所有查询的异步方法会读从库
			//if (this.SlavePools.Count > 0 && this.CurrentThreadTransaction == null) pool = this.SlavePools.Count == 1 ? this.SlavePools[0] : this.SlavePools[slaveRandom.Next(this.SlavePools.Count)];
			if (this.SlavePools.Count > 0 && cmdText.StartsWith("SELECT ", StringComparison.CurrentCultureIgnoreCase)) pool = this.SlavePools.Count == 1 ? this.SlavePools[0] : this.SlavePools[slaveRandom.Next(this.SlavePools.Count)];

			var pc = await PrepareCommandAsync(pool, cmd, cmdType, cmdText, cmdParms);
			string logtxt = pc.logtxt;
			if (IsTracePerformance) logtxt += $"PrepareCommand: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
			Exception ex = null;
			try {
				if (IsTracePerformance) logtxt_dt = DateTime.Now;
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) await cmd.Connection.OpenAsync();
				if (IsTracePerformance) {
					logtxt += $"Open: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
					logtxt_dt = DateTime.Now;
				}
				using (MySqlDataReader dr = await cmd.ExecuteReaderAsync() as MySqlDataReader) {
					if (IsTracePerformance) logtxt += $"ExecuteReader: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
					while (true) {
						if (IsTracePerformance) logtxt_dt = DateTime.Now;
						bool isread = await dr.ReadAsync();
						if (IsTracePerformance) logtxt += $"	dr.Read: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
						if (isread == false) break;

						if (readerHander != null) {
							object[] values = null;
							if (IsTracePerformance) {
								logtxt_dt = DateTime.Now;
								values = new object[dr.FieldCount];
								for (int a = 0; a < values.Length; a++) if (!await dr.IsDBNullAsync(a)) values[a] = await dr.GetFieldValueAsync<object>(a);
								logtxt += $"	dr.GetValues: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";
								logtxt_dt = DateTime.Now;
							}
							await readerHander(dr);
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

			if (IsTracePerformance) logtxt_dt = DateTime.Now;
			pool.ReleaseConnection(pc.conn);
			if (IsTracePerformance) logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			LoggerException(pool, cmd, ex, dt, logtxt);
		}
		/// <summary>
		/// 若使用【读写分离】，查询【从库】条件cmdText.StartsWith("SELECT ")，否则查询【主库】
		/// </summary>
		/// <param name="cmdType"></param>
		/// <param name="cmdText"></param>
		/// <param name="cmdParms"></param>
		/// <returns></returns>
		async public Task<object[][]> ExecuteArrayAsync(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			List<object[]> ret = new List<object[]>();
			await ExecuteReaderAsync(async dr => {
				object[] values = new object[dr.FieldCount];
				for (int a = 0; a < values.Length; a++) if (!await dr.IsDBNullAsync(a)) values[a] = await dr.GetFieldValueAsync<object>(a);
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
		async public Task<int> ExecuteNonQueryAsync(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			var pc = await PrepareCommandAsync(this.MasterPool, cmd, cmdType, cmdText, cmdParms);
			DateTime logtxt_dt = DateTime.Now;
			int val = 0;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) await cmd.Connection.OpenAsync();
				val = await cmd.ExecuteNonQueryAsync();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (IsTracePerformance) logtxt_dt = DateTime.Now;
			this.MasterPool.ReleaseConnection(pc.conn);
			if (IsTracePerformance) pc.logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			LoggerException(this.MasterPool, cmd, ex, dt, pc.logtxt);
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
		async public Task<object> ExecuteScalarAsync(CommandType cmdType, string cmdText, params MySqlParameter[] cmdParms) {
			DateTime dt = DateTime.Now;
			MySqlCommand cmd = new MySqlCommand();
			var pc = await PrepareCommandAsync(this.MasterPool, cmd, cmdType, cmdText, cmdParms);
			DateTime logtxt_dt = DateTime.Now;
			object val = null;
			Exception ex = null;
			try {
				if (cmd.Connection.State == ConnectionState.Closed || cmd.Connection.Ping() == false) await cmd.Connection.OpenAsync();
				val = await cmd.ExecuteScalarAsync();
			} catch (Exception ex2) {
				ex = ex2;
			}

			if (IsTracePerformance) logtxt_dt = DateTime.Now;
			this.MasterPool.ReleaseConnection(pc.conn);
			if (IsTracePerformance) pc.logtxt += $"ReleaseConnection: {DateTime.Now.Subtract(logtxt_dt).TotalMilliseconds}ms Total: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms";
			LoggerException(this.MasterPool, cmd, ex, dt, pc.logtxt);
			cmd.Parameters.Clear();
			return val;
		}

		async private Task<(Connection2 conn, string logtxt)> PrepareCommandAsync(ConnectionPool pool, MySqlCommand cmd, CommandType cmdType, string cmdText, MySqlParameter[] cmdParms) {
			string logtxt = "";
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
			if (IsTracePerformance) logtxt += $"	PrepareCommand_part1: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms cmdParms: {cmdParms.Length}\r\n";

			if (IsTracePerformance) dt = DateTime.Now;
			conn = await pool.GetConnectionAsync();
			cmd.Connection = conn.SqlConnection;
			if (IsTracePerformance) logtxt += $"	PrepareCommand_tran==null: {DateTime.Now.Subtract(dt).TotalMilliseconds}ms\r\n";

			return (conn, logtxt);
		}
	}
}
