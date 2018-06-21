using System;
using System.Collections;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace MySql.Data.MySqlClient {
	public abstract partial class SqlHelper {
		public static Executer Instance { get; } = new Executer();
		public static IDistributedCache Cache { get; private set; }
		public static IConfiguration CacheStrategy { get; private set; }
		private static bool CacheSupportMultiRemove = false;
		protected static void Initialization(IDistributedCache cache, IConfiguration cacheStrategy, string connectionString, ILogger log) {
			Instance.Log = log;
			Instance.Pool.ConnectionString = connectionString;
			Cache = cache;
			CacheStrategy = cacheStrategy;
			if (cache != null) {
				cache.Set("testCacheSupportMultiRemove1", new byte[] { 65 });
				cache.Set("testCacheSupportMultiRemove2", new byte[] { 65 });
				cache.Remove("testCacheSupportMultiRemove1|testCacheSupportMultiRemove2");
				CacheSupportMultiRemove = cache.Get("testCacheSupportMultiRemove1") == null && cache.Get("testCacheSupportMultiRemove2") == null;
				if (CacheSupportMultiRemove == false) log.LogWarning("SqlHelper Warning: 低性能, IDistributedCache 没现实批量删除缓存 Cache.Remove(\"key1|key2\").");
			}
		}
		/// <summary>
		/// 循环或批量删除缓存键，项目启动时检测：Cache.Remove("key1|key2") 若成功删除 key1、key2，说明支持批量删除
		/// </summary>
		/// <param name="keys">缓存键[数组]</param>
		public static void CacheRemove(params string[] keys) {
			if (keys == null || keys.Length == 0) return;
			if (CacheSupportMultiRemove) Cache.Remove(string.Join("|", keys));
			else foreach (var key in keys) Cache.Remove(key);
		}
		/// <summary>
		/// 循环或批量删除缓存键，项目启动时检测：Cache.Remove("key1|key2") 若成功删除 key1、key2，说明支持批量删除
		/// </summary>
		/// <param name="keys">缓存键[数组]</param>
		async public static Task CacheRemoveAsync(params string[] keys) {
			if (keys == null || keys.Length == 0) return;
			if (CacheSupportMultiRemove) await Cache.RemoveAsync(string.Join("|", keys));
			else foreach (var key in keys) await Cache.RemoveAsync(key);
		}

		public static string Addslashes(string filter, params object[] parms) { return Executer.Addslashes(filter, parms); }

		public static void ExecuteReader(Action<MySqlDataReader> readerHander, string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteReader(readerHander, CommandType.Text, cmdText, cmdParms);
		public static object[][] ExeucteArray(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExeucteArray(CommandType.Text, cmdText, cmdParms);
		public static int ExecuteNonQuery(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteNonQuery(CommandType.Text, cmdText, cmdParms);
		public static object ExecuteScalar(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteScalar(CommandType.Text, cmdText, cmdParms);

		public static Task ExecuteReaderAsync(Func<MySqlDataReader, Task> readerHander, string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteReaderAsync(readerHander, CommandType.Text, cmdText, cmdParms);
		public static Task<object[][]> ExeucteArrayAsync(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExeucteArrayAsync(CommandType.Text, cmdText, cmdParms);
		public static Task<int> ExecuteNonQueryAsync(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteNonQueryAsync(CommandType.Text, cmdText, cmdParms);
		public static Task<object> ExecuteScalarAsync(string cmdText, params MySqlParameter[] cmdParms) => Instance.ExecuteScalarAsync(CommandType.Text, cmdText, cmdParms);

		/// <summary>
		/// 开启事务（不支持异步），60秒未执行完将自动提交
		/// </summary>
		/// <param name="handler">事务体 () => {}</param>
		public static void Transaction(Action handler) {
			Transaction(handler, TimeSpan.FromSeconds(60));
		}
		/// <summary>
		/// 开启事务（不支持异步）
		/// </summary>
		/// <param name="handler">事务体 () => {}</param>
		/// <param name="timeout">超时，未执行完将自动提交</param>
		public static void Transaction(Action handler, TimeSpan timeout) {
			try {
				Instance.BeginTransaction(timeout);
				handler();
				Instance.CommitTransaction();
			} catch (Exception ex) {
				Instance.RollbackTransaction();
				throw ex;
			}
		}

		private static DateTime dt1970 = new DateTime(1970, 1, 1);
		private static Random rnd = new Random();
		private static readonly int __staticMachine = ((0x00ffffff & Environment.MachineName.GetHashCode()) +
#if NETSTANDARD1_5 || NETSTANDARD1_6
			1
#else
			AppDomain.CurrentDomain.Id
#endif
			) & 0x00ffffff;
		private static readonly int __staticPid = Process.GetCurrentProcess().Id;
		private static int __staticIncrement = rnd.Next();
		/// <summary>
		/// 生成类似Mongodb的ObjectId有序、不重复Guid
		/// </summary>
		/// <returns></returns>
		public static Guid NewMongodbId() {
			var now = DateTime.Now;
			var uninxtime = (int) now.Subtract(dt1970).TotalSeconds;
			int increment = Interlocked.Increment(ref __staticIncrement) & 0x00ffffff;
			var rand = rnd.Next(0, int.MaxValue);
			var guid = $"{uninxtime.ToString("x8").PadLeft(8, '0')}{__staticMachine.ToString("x8").PadLeft(8, '0').Substring(2, 6)}{__staticPid.ToString("x8").PadLeft(8, '0').Substring(6, 2)}{increment.ToString("x8").PadLeft(8, '0')}{rand.ToString("x8").PadLeft(8, '0')}";
			return Guid.Parse(guid);
		}

		/// <summary>
		/// 缓存壳
		/// </summary>
		/// <typeparam name="T">缓存类型</typeparam>
		/// <param name="key">缓存键</param>
		/// <param name="timeoutSeconds">缓存秒数</param>
		/// <param name="getData">获取源数据的函数</param>
		/// <param name="serialize">序列化函数</param>
		/// <param name="deserialize">反序列化函数</param>
		/// <returns></returns>
		public static T CacheShell<T>(string key, int timeoutSeconds, Func<T> getData, Func<T, string> serialize = null, Func<string, T> deserialize = null) {
			if (timeoutSeconds <= 0) return getData();
			if (Cache == null) throw new Exception("缓存现实 Cache 为 null");
			var cacheValue = Cache.Get(key);
			if (cacheValue != null) {
				try {
					var txt = Encoding.UTF8.GetString(cacheValue);
					return deserialize == null ? JsonConvert.DeserializeObject<T>(txt) : deserialize(txt);
				} catch {
					Cache.Remove(key);
					throw;
				}
			}
			var ret = getData();
			Cache.Set(key, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize(ret)), new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(timeoutSeconds) });
			return ret;
		}
		/// <summary>
		/// 缓存壳(哈希表)
		/// </summary>
		/// <typeparam name="T">缓存类型</typeparam>
		/// <param name="key">缓存键</param>
		/// <param name="field">字段</param>
		/// <param name="timeoutSeconds">缓存秒数</param>
		/// <param name="getData">获取源数据的函数</param>
		/// <param name="serialize">序列化函数</param>
		/// <param name="deserialize">反序列化函数</param>
		/// <returns></returns>
		public static T CacheShell<T>(string key, string field, int timeoutSeconds, Func<T> getData, Func<(T, long), string> serialize = null, Func<string, (T, long)> deserialize = null) {
			if (timeoutSeconds <= 0) return getData();
			if (Cache == null) throw new Exception("缓存现实 Cache 为 null");
			var hashkey = $"{key}:{field}";
			var cacheValue = Cache.Get(hashkey);
			if (cacheValue != null) {
				try {
					var txt = Encoding.UTF8.GetString(cacheValue);
					var value = deserialize == null ? JsonConvert.DeserializeObject<(T, long)>(txt) : deserialize(txt);
					if (DateTime.Now.Subtract(dt1970.AddSeconds(value.Item2)).TotalSeconds <= timeoutSeconds) return value.Item1;
				} catch {
					Cache.Remove(hashkey);
					throw;
				}
			}
			var ret = getData();
			Cache.Set(hashkey, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize((ret, (long) DateTime.Now.Subtract(dt1970).TotalSeconds))));
			return ret;
		}
		/// <summary>
		/// 缓存壳
		/// </summary>
		/// <typeparam name="T">缓存类型</typeparam>
		/// <param name="key">缓存键</param>
		/// <param name="timeoutSeconds">缓存秒数</param>
		/// <param name="getDataAsync">获取源数据的函数</param>
		/// <param name="serialize">序列化函数</param>
		/// <param name="deserialize">反序列化函数</param>
		/// <returns></returns>
		async public static Task<T> CacheShellAsync<T>(string key, int timeoutSeconds, Func<Task<T>> getDataAsync, Func<T, string> serialize = null, Func<string, T> deserialize = null) {
			if (timeoutSeconds <= 0) return await getDataAsync();
			if (Cache == null) throw new Exception("缓存现实 Cache 为 null");
			var cacheValue = await Cache.GetAsync(key);
			if (cacheValue != null) {
				try {
					var txt = Encoding.UTF8.GetString(cacheValue);
					return deserialize == null ? JsonConvert.DeserializeObject<T>(txt) : deserialize(txt);
				} catch {
					await Cache.RemoveAsync(key);
					throw;
				}
			}
			var ret = await getDataAsync();
			await Cache.SetAsync(key, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize(ret)), new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(timeoutSeconds) });
			return ret;
		}
		/// <summary>
		/// 缓存壳(哈希表)
		/// </summary>
		/// <typeparam name="T">缓存类型</typeparam>
		/// <param name="key">缓存键</param>
		/// <param name="field">字段</param>
		/// <param name="timeoutSeconds">缓存秒数</param>
		/// <param name="getDataAsync">获取源数据的函数</param>
		/// <param name="serialize">序列化函数</param>
		/// <param name="deserialize">反序列化函数</param>
		/// <returns></returns>
		async public static Task<T> CacheShellAsync<T>(string key, string field, int timeoutSeconds, Func<Task<T>> getDataAsync, Func<(T, long), string> serialize = null, Func<string, (T, long)> deserialize = null) {
			if (timeoutSeconds <= 0) return await getDataAsync();
			if (Cache == null) throw new Exception("缓存现实 Cache 为 null");
			var hashkey = $"{key}:{field}";
			var cacheValue = await Cache.GetAsync(hashkey);
			if (cacheValue != null) {
				try {
					var txt = Encoding.UTF8.GetString(cacheValue);
					var value = deserialize == null ? JsonConvert.DeserializeObject<(T, long)>(txt) : deserialize(txt);
					if (DateTime.Now.Subtract(dt1970.AddSeconds(value.Item2)).TotalSeconds <= timeoutSeconds) return value.Item1;
				} catch {
					await Cache.RemoveAsync(hashkey);
					throw;
				}
			}
			var ret = await getDataAsync();
			await Cache.SetAsync(hashkey, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize((ret, (long) DateTime.Now.Subtract(dt1970).TotalSeconds))));
			return ret;
		}
	}
}