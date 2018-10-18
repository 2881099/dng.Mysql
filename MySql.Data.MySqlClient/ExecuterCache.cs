using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MySql.Data.MySqlClient {
	partial class Executer {

		public IDistributedCache Cache { get; private set; }
		private bool CacheSupportMultiRemove = false;
		public Executer(IDistributedCache cache, string masterConnectionString, string[] slaveConnectionStrings, ILogger log) {
			Log = log;
			MasterPool = new MySqlConnectionPool("主库", masterConnectionString, null, null);
			Cache = cache;
			if (slaveConnectionStrings != null) {
				foreach (var slaveConnectionString in slaveConnectionStrings) {
					var slavePool = new MySqlConnectionPool($"从库{SlavePools.Count + 1}", slaveConnectionString, () => Interlocked.Decrement(ref slaveUnavailables), () => Interlocked.Increment(ref slaveUnavailables));
					SlavePools.Add(slavePool);
				}
			}
			if (cache != null) {
				var key1 = $"testCacheSupportMultiRemove{Guid.NewGuid().ToString("N")}";
				var key2 = $"testCacheSupportMultiRemove{Guid.NewGuid().ToString("N")}";
				cache.Set(key1, new byte[] { 65 });
				cache.Set(key2, new byte[] { 65 });
				try { cache.Remove($"{key1}|{key2}"); } catch { } // redis-cluster 不允许执行 multi keys 命令
				CacheSupportMultiRemove = cache.Get(key1) == null && cache.Get(key2) == null;
				if (CacheSupportMultiRemove == false) {
					log.LogWarning("PSqlHelper Warning: 低性能, IDistributedCache 没现实批量删除缓存 Cache.Remove(\"key1|key2\").");
					CacheRemove(key1, key2);
				}
			}
		}

		/// <summary>
		/// 循环或批量删除缓存键，项目启动时检测：Cache.Remove("key1|key2") 若成功删除 key1、key2，说明支持批量删除
		/// </summary>
		/// <param name="keys">缓存键[数组]</param>
		public void CacheRemove(params string[] keys) {
			if (keys == null || keys.Length == 0) return;
			var keysDistinct = keys.Distinct();
			if (CacheSupportMultiRemove) Cache.Remove(string.Join("|", keysDistinct));
			else foreach (var key in keysDistinct) Cache.Remove(key);
		}
		/// <summary>
		/// 循环或批量删除缓存键，项目启动时检测：Cache.Remove("key1|key2") 若成功删除 key1、key2，说明支持批量删除
		/// </summary>
		/// <param name="keys">缓存键[数组]</param>
		async public Task CacheRemoveAsync(params string[] keys) {
			if (keys == null || keys.Length == 0) return;
			var keysDistinct = keys.Distinct();
			if (CacheSupportMultiRemove) await Cache.RemoveAsync(string.Join("|", keysDistinct));
			else foreach (var key in keysDistinct) await Cache.RemoveAsync(key);
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
		public T CacheShell<T>(string key, int timeoutSeconds, Func<T> getData, Func<T, string> serialize = null, Func<string, T> deserialize = null) {
			if (timeoutSeconds <= 0) return getData();
			if (Cache == null) throw new Exception("缓存现实 IDistributedCache 为 null");
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
		public T CacheShell<T>(string key, string field, int timeoutSeconds, Func<T> getData, Func<(T, long), string> serialize = null, Func<string, (T, long)> deserialize = null) {
			if (timeoutSeconds <= 0) return getData();
			if (Cache == null) throw new Exception("缓存现实 IDistributedCache 为 null");
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
			var ret = (getData(), (long)DateTime.Now.Subtract(dt1970).TotalSeconds);
            Cache.Set(hashkey, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize(ret)));
			return ret.Item1;
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
		async public Task<T> CacheShellAsync<T>(string key, int timeoutSeconds, Func<Task<T>> getDataAsync, Func<T, string> serialize = null, Func<string, T> deserialize = null) {
			if (timeoutSeconds <= 0) return await getDataAsync();
			if (Cache == null) throw new Exception("缓存现实 IDistributedCache 为 null");
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
		async public Task<T> CacheShellAsync<T>(string key, string field, int timeoutSeconds, Func<Task<T>> getDataAsync, Func<(T, long), string> serialize = null, Func<string, (T, long)> deserialize = null) {
			if (timeoutSeconds <= 0) return await getDataAsync();
			if (Cache == null) throw new Exception("缓存现实 IDistributedCache 为 null");
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
			var ret = (await getDataAsync(), (long)DateTime.Now.Subtract(dt1970).TotalSeconds);
            await Cache.SetAsync(hashkey, Encoding.UTF8.GetBytes(serialize == null ? JsonConvert.SerializeObject(ret) : serialize(ret)));
			return ret.Item1;
		}
	}
}