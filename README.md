对 Mysql.Data 进行的二次封装，包含连接池、缓存

也是dotnetgen_mysql生成器所需mysql数据库基础封装

# 安装

> Install-Package dng.Mysql

# 使用

```csharp
public static MySql.Data.MySqlClient.Executer MysqlInstance = 
    new MySql.Data.MySqlClient.Executer(IDistributedCache, connectionString, ILogger);

//MysqlInstance.ExecuteReader
//MysqlInstance.ExecuteReaderAsync

//ExecuteArray
//ExecuteArrayAsync

//ExecuteNonQuery
//ExecuteNonQueryAsync

//ExecuteScalar
//ExecuteScalarAsync
```

# 事务

```csharp
MysqlInstance.Transaction(() => {

});
```

# 缓存壳

```csharp
MysqlInstance.CacheShell(key, timeoutSeconds, () => {
    return dataSource;
});

MysqlInstance.RemoveCache(key);
```