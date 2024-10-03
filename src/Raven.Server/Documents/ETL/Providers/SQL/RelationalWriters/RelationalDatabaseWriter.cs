﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Extensions.Streams;
using Raven.Client.Util;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    public sealed class RelationalDatabaseWriter : RelationalDatabaseWriterBase, IDisposable
    {
        private readonly RavenLogger _logger;

        private readonly SqlEtl _etl;
        private readonly DocumentDatabase _database;

        private readonly DbCommandBuilder _commandBuilder;
        private readonly DbProviderFactory _providerFactory;
        private readonly DbConnection _connection;
        private readonly DbTransaction _tx;

        private readonly List<Func<DbParameter, string, bool>> _stringParserList;
        private readonly SqlProvider _providerType;

        private const int LongStatementWarnThresholdInMs = 3000;

        public RelationalDatabaseWriter(SqlEtl etl, DocumentDatabase database)
            : base(etl.Configuration.Connection.FactoryName)
        {
            _etl = etl;
            _database = database;
            _logger = RavenLogManager.Instance.GetLoggerForDatabase<RelationalDatabaseWriter>(database);
            _providerFactory = GetDbProviderFactory(etl.Configuration);
            _providerType = SqlProviderParser.GetSupportedProvider(_etl.Configuration.Connection.FactoryName);
            _commandBuilder = _providerFactory.InitializeCommandBuilder();
            _connection = _providerFactory.CreateConnection();
            var connectionString = etl.Configuration.Connection.ConnectionString;
            _connection.ConnectionString = connectionString;

            OpenConnection(database, etl.Configuration.Name, etl.Configuration.ConnectionStringName);

            _tx = _connection.BeginTransaction();

            _stringParserList = GenerateStringParsers();
        }

        private void OpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName)
        {
            const int maxRetries = 5;
            var retries = 0;

            while (true)
            {
                try
                {
                    _connection.Open();
                    return;
                }
                catch (Exception e)
                {
                    if (++retries < maxRetries)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Failed to open connection, retrying ({retries}/{maxRetries})", e);

                        Thread.Sleep(50);
                        continue;
                    }

                    using (_connection)
                    {
                        database.NotificationCenter.Add(AlertRaised.Create(
                            database.Name,
                            SqlEtl.SqlEtlTag,
                            $"[{etlConfigurationName}] Could not open connection using '{connectionStringName}' connection string",
                            AlertType.SqlEtl_ConnectionError,
                            NotificationSeverity.Error,
                            key: $"{etlConfigurationName}/{connectionStringName}",
                            details: new ExceptionDetails(e)));

                        throw;
                    }
                }
            }
        }

        public static void TestConnection(string factoryName, string connectionString)
        {
            var providerFactory = DbProviderFactories.GetFactory(factoryName);
            var connection = providerFactory.CreateConnection();
            connection.ConnectionString = connectionString;

            try
            {
                connection.Open();
                connection.Close();
            }
            finally
            {
                connection.Dispose();
            }
        }

        private DbProviderFactory GetDbProviderFactory(SqlEtlConfiguration configuration)
        {
            DbProviderFactory providerFactory;

            try
            {
                providerFactory = DbProviderFactories.GetFactory(configuration.Connection.FactoryName);
            }
            catch (Exception e)
            {
                var message = $"Could not find provider factory {configuration.Connection.FactoryName} to replicate to sql for {configuration.Name}, ignoring.";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    SqlEtl.SqlEtlTag,
                    message,
                    AlertType.SqlEtl_ProviderError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
            return providerFactory;
        }

        public void Dispose()
        {
            using (_connection)
            using (_tx)
            {

            }
        }

        public void Commit()
        {
            _tx.Commit();
        }

        public void Rollback()
        {
            _tx.Rollback();
        }

        private int InsertItems(string tableName, string pkName, List<ToSqlItem> toInsert, Action<DbCommand> commandCallback, CancellationToken token)
        {
            var inserted = 0;

            var sp = new Stopwatch();
            foreach (var itemToReplicate in toInsert)
            {
                sp.Restart();

                using (var cmd = CreateCommand())
                using (token.Register(cmd.Cancel))
                {
                    token.ThrowIfCancellationRequested();

                    var sb = new StringBuilder("INSERT INTO ")
                        .Append(GetTableNameString(tableName))
                        .Append(" (")
                        .Append(_commandBuilder.QuoteIdentifier(pkName))
                        .Append(", ");
                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Id == pkName)
                            continue;
                        sb.Append(_commandBuilder.QuoteIdentifier(column.Id)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;

                    var pkParam = cmd.CreateParameter();

                    pkParam.ParameterName = GetParameterName(pkName);
                    pkParam.Value = itemToReplicate.DocumentId.ToString();
                    cmd.Parameters.Add(pkParam);

                    sb.Append(") \r\nVALUES (")
                        .Append(GetParameterName(pkName))
                        .Append(", ");

                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Id == pkName)
                            continue;
                        var colParam = cmd.CreateParameter();
                        colParam.ParameterName = GetParameterName(column.Id);
                        SetParamValue(colParam, column, _stringParserList, _providerType);
                        cmd.Parameters.Add(colParam);
                        sb.Append(GetParameterName(column.Id)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;
                    sb.Append(")");

                    if (IsSqlServerFactoryType && _etl.Configuration.ForceQueryRecompile)
                    {
                        sb.Append(" OPTION(RECOMPILE)");
                    }

                    var stmt = sb.ToString();
                    cmd.CommandText = stmt;

                    commandCallback?.Invoke(cmd);

                    try
                    {
                        cmd.ExecuteNonQuery();
                        inserted++;
                    }
                    catch (Exception e)
                    {
                        if (token.IsCancellationRequested == false)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info(
                                    $"Failed to replicate changes to relational database for: {_etl.Name} " +
                                    $"(doc: {itemToReplicate.DocumentId}), will continue trying. {Environment.NewLine}{cmd.CommandText}", e);
                            }

                            _etl.Statistics.RecordPartialLoadError(
                                $"Insert statement:{Environment.NewLine}{cmd.CommandText}{Environment.NewLine}. Error:{Environment.NewLine}{e}",
                                itemToReplicate.DocumentId);
                        }
                    }
                    finally
                    {
                        sp.Stop();

                        var elapsedMilliseconds = sp.ElapsedMilliseconds;

                        if (_logger.IsInfoEnabled && token.IsCancellationRequested == false)
                            _logger.Info($"Insert took: {elapsedMilliseconds:#,#;;0}ms, statement: {stmt}");

                        var tableMetrics = _etl.SqlMetrics.GetTableMetrics(tableName);
                        tableMetrics.InsertActionsMeter.MarkSingleThreaded(1);

                        if (elapsedMilliseconds > LongStatementWarnThresholdInMs)
                        {
                            HandleSlowSql(elapsedMilliseconds, stmt);
                        }
                    }
                }
            }

            return inserted;
        }

        private DbCommand CreateCommand()
        {
            var cmd = _connection.CreateCommand();

            try
            {
                cmd.Transaction = _tx;

                if (_etl.Configuration.CommandTimeout.HasValue)
                    cmd.CommandTimeout = _etl.Configuration.CommandTimeout.Value;
                else if (_database.Configuration.Etl.SqlCommandTimeout.HasValue)
                    cmd.CommandTimeout = (int)_database.Configuration.Etl.SqlCommandTimeout.Value.AsTimeSpan.TotalSeconds;

                return cmd;
            }
            catch (Exception)
            {
                cmd.Dispose();
                throw;
            }
        }

        public int DeleteItems(string tableName, string pkName, bool parameterize, List<ToSqlItem> toDelete, Action<DbCommand> commandCallback, CancellationToken token)
        {
            const int maxParams = 1000;

            var deleted = 0;

            var sp = new Stopwatch();
            using (var cmd = CreateCommand())
            using (token.Register(cmd.Cancel))
            {
                sp.Start();
                token.ThrowIfCancellationRequested();

                for (int i = 0; i < toDelete.Count; i += maxParams)
                {
                    cmd.Parameters.Clear();
                    var sb = new StringBuilder("DELETE FROM ")
                        .Append(GetTableNameString(tableName))
                        .Append(" WHERE ")
                        .Append(_commandBuilder.QuoteIdentifier(pkName))
                        .Append(" IN (");

                    var countOfDeletes = 0;
                    for (int j = i; j < Math.Min(i + maxParams, toDelete.Count); j++)
                    {
                        if (i != j)
                            sb.Append(", ");

                        if (parameterize)
                        {
                            var dbParameter = cmd.CreateParameter();
                            dbParameter.ParameterName = GetParameterName("p" + j);
                            dbParameter.Value = toDelete[j].DocumentId.ToString();
                            cmd.Parameters.Add(dbParameter);
                            sb.Append(dbParameter.ParameterName);
                        }
                        else
                        {
                            sb.Append("'").Append(SanitizeSqlValue(toDelete[j].DocumentId)).Append("'");
                        }

                        if (toDelete[j].IsDelete) // count only "real" deletions, not the ones because of insert
                            countOfDeletes++;
                    }
                    sb.Append(")");

                    if (IsSqlServerFactoryType && _etl.Configuration.ForceQueryRecompile)
                    {
                        sb.Append(" OPTION(RECOMPILE)");
                    }
                    var stmt = sb.ToString();
                    cmd.CommandText = stmt;

                    commandCallback?.Invoke(cmd);

                    try
                    {
                        cmd.ExecuteNonQuery();

                        deleted += countOfDeletes;
                    }
                    catch (Exception e)
                    {
                        if (token.IsCancellationRequested == false)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Failure to replicate deletions to relational database for: {_etl.Name}, " +
                                             "will continue trying." + Environment.NewLine + cmd.CommandText, e);

                            _etl.Statistics.RecordPartialLoadError($"Delete statement:{Environment.NewLine}{cmd.CommandText}{Environment.NewLine}Error:{Environment.NewLine}{e}",
                                null);
                        }
                    }
                    finally
                    {
                        sp.Stop();

                        var elapsedMilliseconds = sp.ElapsedMilliseconds;

                        if (_logger.IsInfoEnabled && token.IsCancellationRequested == false)
                            _logger.Info($"Delete took: {elapsedMilliseconds:#,#;;0}ms, statement: {stmt}");

                        var tableMetrics = _etl.SqlMetrics.GetTableMetrics(tableName);
                        tableMetrics.DeleteActionsMeter.MarkSingleThreaded(1);

                        if (elapsedMilliseconds > LongStatementWarnThresholdInMs)
                        {
                            HandleSlowSql(elapsedMilliseconds, stmt);
                        }
                    }
                }
            }

            return deleted;
        }

        private void HandleSlowSql(long elapsedMilliseconds, string stmt)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"[{_etl.Name}] Slow SQL detected. Execution took: {elapsedMilliseconds:#,#;;0}ms, statement: {stmt}");

            _etl.Statistics.RecordSlowSql(new SlowSqlStatementInfo
            {
                Date = SystemTime.UtcNow,
                Duration = elapsedMilliseconds,
                Statement = stmt
            });
        }

        private string GetTableNameString(string tableName)
        {
            if (_etl.Configuration.QuoteTables)
            {
                return string.Join(".", tableName.Split('.').Select(_commandBuilder.QuoteIdentifier).ToArray());
            }

            return tableName;
        }

        public static string SanitizeSqlValue(string sqlValue)
        {
            return sqlValue.Replace("'", "''");
        }

        private string GetParameterName(string paramName)
        {
            switch (_providerFactory.GetType().Name)
            {
                case "SqlClientFactory":
                case "MySqlClientFactory":
                case "MySqlConnectorFactory":
                    return "@" + paramName;

                case "OracleClientFactory":
                case "NpgsqlFactory":
                    return ":" + paramName;

                default:
                    throw new NotSupportedException($"Unhandled provider factory: {_providerFactory.GetType().Name}");
            }
        }

        public SqlWriteStats Write(SqlTableWithRecords table, List<DbCommand> commands, CancellationToken token)
        {
            var stats = new SqlWriteStats();

            var collectCommands = commands != null ? commands.Add : (Action<DbCommand>)null;

            if (table.InsertOnlyMode == false && table.Deletes.Count > 0)
            {
                // first, delete all the rows that might already exist there
                stats.DeletedRecordsCount = DeleteItems(table.TableName, table.DocumentIdColumn, _etl.Configuration.ParameterizeDeletes, table.Deletes, collectCommands, token);
            }

            if (table.Inserts.Count > 0)
            {
                stats.InsertedRecordsCount = InsertItems(table.TableName, table.DocumentIdColumn, table.Inserts, collectCommands, token);
            }

            return stats;
        }

        public static void SetParamValue(DbParameter colParam, SqlColumn column, List<Func<DbParameter, string, bool>> stringParsers, SqlProvider provider)
        {
            if (column.Value == null)
                colParam.Value = DBNull.Value;
            else
            {
                switch (column.Type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        colParam.Value = DBNull.Value;
                        break;
                    case BlittableJsonToken.Boolean:
                    case BlittableJsonToken.Integer:
                        colParam.Value = column.Value;
                        break;
                    case BlittableJsonToken.LazyNumber:
                        colParam.Value = (double)(LazyNumberValue)column.Value;
                        break;

                    case BlittableJsonToken.String:
                        SetParamStringValue(colParam, ((LazyStringValue)column.Value).ToString(), stringParsers);
                        break;
                    case BlittableJsonToken.CompressedString:
                        SetParamStringValue(colParam, ((LazyCompressedStringValue)column.Value).ToString(), stringParsers);
                        break;

                    case BlittableJsonToken.StartObject:
                        var objectValue = (BlittableJsonReaderObject)column.Value;
                        if (objectValue.Count >= 2)
                        {
                            if (objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Type), out object dbType) &&
                                objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Value), out object fieldValue))
                            {
                                var dbTypeString = dbType.ToString() ?? string.Empty;

                                bool useGenericDbType = Enum.TryParse(dbTypeString, ignoreCase: false, out DbType type);

                                if (useGenericDbType)
                                {
                                    var value = fieldValue.ToString();

                                    try
                                    {
                                        colParam.DbType = type;
                                    }
                                    catch
                                    {
                                        if (type == DbType.Guid && Guid.TryParse(value, out var guid1) && colParam is OracleParameter oracleParameter)
                                        {
                                            var arr = guid1.ToByteArray();
                                            oracleParameter.Value = arr;
                                            oracleParameter.OracleDbType = OracleDbType.Raw;
                                            oracleParameter.Size = arr.Length;
                                            break;
                                        }

                                        throw;
                                    }

                                    if (colParam.DbType == DbType.Guid && Guid.TryParse(value, out var guid))
                                    {
                                        if (colParam is Npgsql.NpgsqlParameter || colParam is SqlParameter)
                                            colParam.Value = guid;

                                        if (colParam is MySqlConnector.MySqlParameter mySqlConnectorParameter) 
                                        {
                                            var arr = guid.ToByteArray();
                                            mySqlConnectorParameter.Value = arr;
                                            mySqlConnectorParameter.MySqlDbType = MySqlConnector.MySqlDbType.Binary;
                                            mySqlConnectorParameter.Size = arr.Length;
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        colParam.Value = value;
                                    }
                                }
                                else
                                {
                                    // failed to parse db type - try to fallback to provider specific type

                                    switch (provider)
                                    {
                                        case SqlProvider.SqlClient:
                                            SqlDbType sqlDbType = ParseProviderSpecificParameterType<SqlDbType>(dbTypeString);
                                            ((SqlParameter)colParam).SqlDbType = sqlDbType;
                                            break;
                                        case SqlProvider.Npgsql:
                                            NpgsqlDbType npgsqlType = ParseProviderSpecificParameterType<NpgsqlDbType>(dbTypeString);
                                            ((Npgsql.NpgsqlParameter)colParam).NpgsqlDbType = npgsqlType;
                                            break;
                                        case SqlProvider.MySqlClient:
                                        case SqlProvider.MySqlConnectorFactory:
                                            MySqlConnector.MySqlDbType mySqlConnectorDbType = ParseProviderSpecificParameterType<MySqlConnector.MySqlDbType>(dbTypeString);
                                            ((MySqlConnector.MySqlParameter)colParam).MySqlDbType = mySqlConnectorDbType;
                                            break;
                                        case SqlProvider.OracleClient:
                                            OracleDbType oracleDbType = ParseProviderSpecificParameterType<OracleDbType>(dbTypeString);
                                            ((OracleParameter)colParam).OracleDbType = oracleDbType;
                                            break;
                                        default:
                                            ThrowProviderNotSupported();
                                            break;
                                    }

                                    if (fieldValue is IEnumerable<object> enumerableValue)
                                    {
                                        Type detectedType = null;

                                        colParam.Value = enumerableValue.Select(x =>
                                        {
                                            if (x is IConvertible)
                                            {
                                                detectedType ??= TryDetectCollectionType(dbTypeString, x);

                                                if (detectedType != null)
                                                    return Convert.ChangeType(x, detectedType);

                                                return x.ToString();
                                            }

                                            return x.ToString();
                                        }).ToArray();
                                    }
                                    else
                                    {
                                        colParam.Value = fieldValue.ToString();
                                    }
                                }

                                if (objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Size), out object size))
                                {
                                    colParam.Size = (int)(long)size;
                                }
                                break;
                            }
                        }
                        colParam.Value = objectValue.ToString();
                        break;
                    case BlittableJsonToken.StartArray:
                        var blittableJsonReaderArray = (BlittableJsonReaderArray)column.Value;
                        colParam.Value = blittableJsonReaderArray.ToString();
                        break;
                    default:
                        {
                            if (column.Value is Stream stream)
                            {
                                colParam.DbType = DbType.Binary;

                                if (stream == Stream.Null)
                                    colParam.Value = DBNull.Value;
                                else
                                    colParam.Value = stream.ReadData();

                                break;
                            }
                            throw new InvalidOperationException("Cannot understand how to save " + column.Type + " for " + colParam.ParameterName);
                        }
                }
            }

            void ThrowProviderNotSupported()
            {
                throw new NotSupportedException($"Factory provider '{provider}' is not supported");
            }

            Type TryDetectCollectionType(string dbTypeString, object value)
            {
                Type detectedType = null;

                string lowerFieldType = dbTypeString.ToLower();

                if (value is LazyStringValue or LazyCompressedStringValue)
                {
                    if (lowerFieldType.Contains("time") || lowerFieldType.Contains("date"))
                        detectedType = typeof(DateTime);
                    else
                        detectedType = typeof(string);
                }
                else if (value is LazyNumberValue or long or double)
                {
                    if (lowerFieldType.Contains("double"))
                        detectedType = typeof(double);
                    else if (lowerFieldType.Contains("decimal"))
                        detectedType = typeof(decimal);
                    else if (lowerFieldType.Contains("float"))
                        detectedType = typeof(float);
                    else if (lowerFieldType.Contains("bigint"))
                        detectedType = typeof(long);
                    else if (lowerFieldType.Contains("int"))
                        detectedType = typeof(int);
                    else if (lowerFieldType.Contains("decimal") || lowerFieldType.Contains("money") || lowerFieldType.Contains("numeric"))
                        detectedType = typeof(decimal);
                }

                return detectedType;
            }
        }

        private static T ParseProviderSpecificParameterType<T>(string dbTypeString) where T : struct, Enum, IConvertible
        {
            if (dbTypeString.Contains("|"))
            {
                var multipleTypes = dbTypeString.Split('|').Select(e =>
                {
                    if (Enum.TryParse(e.Trim(), ignoreCase: true, out T singleProviderSpecificType) == false)
                        ThrowCouldNotParseDbType();

                    return singleProviderSpecificType;
                }).ToList();

                return multipleTypes.Aggregate((a, b) => (T)Enum.ToObject(typeof(T), Convert.ToInt32(a) | Convert.ToInt32(b)));
            }

            if (Enum.TryParse(dbTypeString, ignoreCase: true, out T providerSpecificType) == false)
                ThrowCouldNotParseDbType();

            return providerSpecificType;

            void ThrowCouldNotParseDbType()
            {
                throw new InvalidOperationException(string.Format($"Couldn't parse '{dbTypeString}' as db type."));
            }
        }

        private static void SetParamStringValue(DbParameter colParam, string value, List<Func<DbParameter, string, bool>> stringParsers)
        {
            if (value.Length > 0 && stringParsers != null)
            {
                foreach (var parser in stringParsers)
                {
                    if (parser(colParam, value))
                    {
                        return;
                    }
                }
            }
            colParam.Value = value;
        }

        public List<Func<DbParameter, string, bool>> GenerateStringParsers()
        {
            return new List<Func<DbParameter, string, bool>>
            {
                (colParam, value) =>
                {
                    if (char.IsDigit(value[0]))
                    {
                        if (DateTime.TryParseExact(value, DefaultFormat.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime dateTime))
                        {
                            colParam.Value = dateTime;
                            return true;
                        }
                    }
                    return false;
                },
                (colParam, value) =>
                {
                    if (char.IsDigit(value[0]))
                    {
                        if (DateTimeOffset.TryParseExact(value, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind, out DateTimeOffset dateTimeOffset))
                        {
                            colParam.Value = dateTimeOffset;
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
    }
}
