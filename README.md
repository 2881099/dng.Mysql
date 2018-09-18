对 Mysql.Data 进行的二次封装，包含连接池、缓存

也是dotnetgen_mysql生成器所需mysql数据库基础封装

# 安装

> Install-Package dng.Mysql

# 使用

```csharp
public static MySql.Data.MySqlClient.Executer MysqlInstance = 
    new MySql.Data.MySqlClient.Executer(IDistributedCache, masterConnectionString, slaveConnectionStrings, ILogger);

MysqlInstance.ExecuteReader
MysqlInstance.ExecuteReaderAsync

MysqlInstance.ExecuteArray
MysqlInstance.ExecuteArrayAsync

MysqlInstance.ExecuteNonQuery
MysqlInstance.ExecuteNonQueryAsync

MysqlInstance.ExecuteScalar
MysqlInstance.ExecuteScalarAsync
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

# 读写分离

若配置了从数据库连接串，从数据库可以设置多个，访问策略为随机。从库实现了故障切换，自动恢复机制。

以下方法执行 sql 语句，为 select 开头，则默认查从数据库，反之则查主数据库。

MysqlInstance.ExecuteReader
MysqlInstance.ExecuteReaderAsync

MysqlInstance.ExecuteArray
MysqlInstance.ExecuteArrayAsync

以下方法在主数据库执行：

```csharp
MysqlInstance.ExecuteNonQuery
MysqlInstance.ExecuteNonQueryAsync

MysqlInstance.ExecuteScalar
MysqlInstance.ExecuteScalarAsync
```
