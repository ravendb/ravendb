using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Bundles.SqlReplication
{
    public class RelationalDatabaseWriter : IDisposable
    {
        private static string[] SqlServerFactoryNames =
        {
            "System.Data.SqlClient",
            "System.Data.SqlServerCe.4.0",
            "MySql.Data.MySqlClient",
            "System.Data.SqlServerCe.3.5"
        };

        private readonly DocumentDatabase database;
        private readonly SqlReplicationConfig cfg;
        private readonly DbProviderFactory providerFactory;
        private readonly SqlReplicationStatistics replicationStatistics;
        private readonly DbCommandBuilder commandBuilder;
        private readonly DbConnection connection;
        private readonly DbTransaction tx;
        private readonly List<Func<DbParameter, String, Boolean>> stringParserList;
        private bool IsSqlServerFactoryType = false;
        private SqlReplicationMetricsCountersManager sqlReplicationMetrics;

        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private const int LongStatementWarnThresholdInMiliseconds = 3000;

        bool hadErrors;

        public static void TestConnection(string factoryName, string connectionString)
        {
            var providerFactory = DbProviderFactories.GetFactory(factoryName);
            var connection = providerFactory.CreateConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            connection.Close();
        }

        public RelationalDatabaseWriter(DocumentDatabase database, SqlReplicationConfig cfg, SqlReplicationStatistics replicationStatistics)
        {
            this.database = database;
            this.cfg = cfg;
            this.replicationStatistics = replicationStatistics;

            providerFactory = GetDbProviderFactory(cfg);

            commandBuilder = providerFactory.CreateCommandBuilder();
            connection = providerFactory.CreateConnection();

            Debug.Assert(connection != null);
            Debug.Assert(commandBuilder != null);

            connection.ConnectionString = cfg.ConnectionString;

            if (SqlServerFactoryNames.Contains(cfg.FactoryName))
            {
                IsSqlServerFactoryType = true;
            }

            try
            {
                connection.Open();
            }
            catch (Exception e)
            {
                var message = "Sql Replication could not open connection to " + connection.ConnectionString;
                log.Error(message);
                database.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = SystemTime.UtcNow,
                    Exception = e.ToString(),
                    Title = "Sql Replication could not open connection",
                    Message = message,
                    UniqueKey = "Sql Replication Connection Error: " + connection.ConnectionString
                });
                throw;
            }

            tx = connection.BeginTransaction();

            stringParserList = GenerateStringParsers();
            sqlReplicationMetrics = database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault().GetSqlReplicationMetricsManager(cfg);
        }

        public List<Func<DbParameter, string, bool>> GenerateStringParsers()
        {
            return new List<Func<DbParameter, string, bool>> {
                (colParam, value) => {
                    if( char.IsDigit( value[ 0 ] ) ) {
                            DateTime dateTime;
                            if (DateTime.TryParseExact(value, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
                                                        DateTimeStyles.RoundtripKind, out dateTime))
                            {
                                switch( providerFactory.GetType( ).Name ) {
                                    case "MySqlClientFactory":
                                        colParam.Value = dateTime.ToString( "yyyy-MM-dd HH:mm:ss.ffffff" );
                                        break;
                                    default:
                                        colParam.Value = dateTime;
                                        break;
                                }
                                return true;
                            }
                    }
                    return false;
                },
                (colParam, value) => {
                    if( char.IsDigit( value[ 0 ] ) ) {
                        DateTimeOffset dateTimeOffset;
                        if( DateTimeOffset.TryParseExact( value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                                                         DateTimeStyles.RoundtripKind, out dateTimeOffset ) ) {
                            switch( providerFactory.GetType( ).Name ) {
                                case "MySqlClientFactory":
                                    colParam.Value = dateTimeOffset.ToUniversalTime().ToString( "yyyy-MM-dd HH:mm:ss.ffffff" );
                                    break;
                                default:
                                    colParam.Value = dateTimeOffset;
                                    break;
                            }
                            return true;
                        }
                    }
                    return false;
                }
            };
        }

        public bool Execute(ConversionScriptResult scriptResult)
        {
            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
            {
                if (sqlReplicationTable.InsertOnlyMode)
                    continue;
                // first, delete all the rows that might already exist there
                DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled,
                                        scriptResult.Ids);
            }

            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
            {
                List<ItemToReplicate> dataForTable;
                if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
                    continue;

                InsertItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable);
            }

            Commit();

            return hadErrors == false;
        }

        public class TableQuerySummary
        {
            public string TableName { get; set; }
            public CommandData[] Commands { get; set; }


            public class CommandData
            {
                public string CommandText { get; set; }
                public KeyValuePair<string, object>[] Params { get; set; }
            }

            public static TableQuerySummary GenerateSummaryFromCommands(string tableName, IEnumerable<DbCommand> commands)
            {
                var tableQuerySummary = new TableQuerySummary();
                tableQuerySummary.TableName = tableName;
                tableQuerySummary.Commands =
                    commands
                        .Select(x => new CommandData()
                        {
                            CommandText = x.CommandText,
                            Params = x.Parameters.Cast<DbParameter>().Select(y => new KeyValuePair<string, object>(y.ParameterName, y.Value)).ToArray()
                        }).ToArray();

                return tableQuerySummary;
            }
        }

        public IEnumerable<TableQuerySummary> RolledBackExecute(ConversionScriptResult scriptResult)
        {
            var identifiers = scriptResult.Data.SelectMany(x => x.Value).Select(x => x.DocumentId).Distinct().ToList();

            // first, delete all the rows that might already exist there
            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
            {
                var commands = new List<DbCommand>();
                DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled,
                    identifiers, commands.Add);
                yield return TableQuerySummary.GenerateSummaryFromCommands(sqlReplicationTable.TableName, commands);

            }

            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
            {
                List<ItemToReplicate> dataForTable;
                if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
                    continue;
                var commands = new List<DbCommand>();
                InsertItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable, commands.Add);

                yield return TableQuerySummary.GenerateSummaryFromCommands(sqlReplicationTable.TableName, commands);
            }

            Rollback();

        }

        public bool Commit()
        {
            tx.Commit();
            return true;
        }

        public bool Rollback()
        {
            tx.Rollback();
            return true;
        }

        private void InsertItems(string tableName, string pkName, List<ItemToReplicate> dataForTable, Action<DbCommand> commandCallback = null)
        {
            var sqlReplicationTableMetrics = sqlReplicationMetrics.GetTableMetrics(tableName);
            var replicationInsertActionsMetrics = sqlReplicationTableMetrics.SqlReplicationInsertActionsMeter;
            var replicationInsertActionsHistogram = sqlReplicationTableMetrics.SqlReplicationInsertActionsHistogram;
            var replicationInsertDurationHistogram = sqlReplicationTableMetrics.SqlReplicationInsertActionsDurationHistogram;

            var sp = new Stopwatch();
            foreach (var itemToReplicate in dataForTable)
            {
                sp.Restart();
                using (var cmd = connection.CreateCommand())
                {
                    cmd.Transaction = tx;

                    if (cfg.CommandTimeout.HasValue)
                        cmd.CommandTimeout = cfg.CommandTimeout.Value;
                    else if (database.Configuration.SqlReplication.CommandTimeoutInSec >= 0)
                        cmd.CommandTimeout = database.Configuration.SqlReplication.CommandTimeoutInSec;

                    database.WorkContext.CancellationToken.ThrowIfCancellationRequested();

                    var sb = new StringBuilder("INSERT INTO ")
                        .Append(GetTableNameString(tableName))
                        .Append(" (")
                        .Append(commandBuilder.QuoteIdentifier(pkName))
                        .Append(", ");
                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Key == pkName)
                            continue;
                        sb.Append(commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;

                    var pkParam = cmd.CreateParameter();

                    pkParam.ParameterName = GetParameterName(providerFactory, commandBuilder, pkName);
                    pkParam.Value = itemToReplicate.DocumentId;
                    cmd.Parameters.Add(pkParam);

                    sb.Append(") \r\nVALUES (")
                        .Append(GetParameterName(providerFactory, commandBuilder, pkName))
                        .Append(", ");

                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Key == pkName)
                            continue;
                        var colParam = cmd.CreateParameter();
                        colParam.ParameterName = column.Key;
                        SetParamValue(colParam, column.Value, stringParserList);
                        cmd.Parameters.Add(colParam);
                        sb.Append(GetParameterName(providerFactory, commandBuilder, column.Key)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;
                    sb.Append(")");

                    if (IsSqlServerFactoryType && cfg.ForceSqlServerQueryRecompile)
                    {
                        sb.Append(" OPTION(RECOMPILE)");
                    }

                    var stmt = sb.ToString();
                    cmd.CommandText = stmt;

                    if (commandCallback != null)
                    {
                        commandCallback(cmd);
                    }
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        log.WarnException(
                            "Failure to replicate changes to relational database for: " + cfg.Name + " (doc: " + itemToReplicate.DocumentId + " ), will continue trying." +
                            Environment.NewLine + cmd.CommandText, e);
                        replicationStatistics.RecordWriteError(e, database);
                        hadErrors = true;
                    }
                    finally
                    {
                        sp.Stop();

                        var elapsedMiliseconds = sp.ElapsedMilliseconds;

                        if (log.IsDebugEnabled)
                        {
                            log.Debug(string.Format("Insert took: {0}ms, statement: {1}", elapsedMiliseconds, stmt));
                        }

                        var elapsedMicroseconds = (long)(sp.ElapsedTicks * SystemTime.MicroSecPerTick);
                        replicationInsertDurationHistogram.Update(elapsedMicroseconds);
                        replicationInsertActionsMetrics.Mark(1);
                        replicationInsertActionsHistogram.Update(1);

                        if (elapsedMiliseconds > LongStatementWarnThresholdInMiliseconds)
                        {
                            HandleSlowSql(elapsedMiliseconds, stmt);
                        }
                    }
                }
            }
        }

        public void DeleteItems(string tableName, string pkName, bool doNotParameterize, List<string> identifiers, Action<DbCommand> commandCallback = null)
        {
            const int maxParams = 1000;
            var sqlReplicationTableMetrics = sqlReplicationMetrics.GetTableMetrics(tableName);
            var replicationDeleteDurationHistogram = sqlReplicationTableMetrics.SqlReplicationDeleteActionsDurationHistogram;
            var replicationDeletesActionsMetrics = sqlReplicationTableMetrics.SqlReplicationDeleteActionsMeter;
            var replicationDeletesActionsHistogram = sqlReplicationTableMetrics.SqlReplicationDeleteActionsHistogram;

            var sp = new Stopwatch();
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = tx;

                if (cfg.CommandTimeout.HasValue)
                    cmd.CommandTimeout = cfg.CommandTimeout.Value;
                else if (database.Configuration.SqlReplication.CommandTimeoutInSec >= 0)
                    cmd.CommandTimeout = database.Configuration.SqlReplication.CommandTimeoutInSec;

                database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
                for (int i = 0; i < identifiers.Count; i += maxParams)
                {
                    sp.Restart();
                    cmd.Parameters.Clear();
                    var sb = new StringBuilder("DELETE FROM ")
                        .Append(GetTableNameString(tableName))
                        .Append(" WHERE ")
                        .Append(commandBuilder.QuoteIdentifier(pkName))
                        .Append(" IN (");

                    for (int j = i; j < Math.Min(i + maxParams, identifiers.Count); j++)
                    {
                        if (i != j)
                            sb.Append(", ");
                        if (doNotParameterize == false)
                        {
                            var dbParameter = cmd.CreateParameter();
                            dbParameter.ParameterName = GetParameterName(providerFactory, commandBuilder, "p" + j);
                            dbParameter.Value = identifiers[j];
                            cmd.Parameters.Add(dbParameter);
                            sb.Append(dbParameter.ParameterName);
                        }
                        else
                        {
                            sb.Append("'").Append(SanitizeSqlValue(identifiers[j])).Append("'");
                        }

                    }
                    sb.Append(")");

                    if (IsSqlServerFactoryType && cfg.ForceSqlServerQueryRecompile)
                    {
                        sb.Append(" OPTION(RECOMPILE)");
                    }
                    var stmt = sb.ToString();
                    cmd.CommandText = stmt;

                    if (commandCallback != null)
                    {
                        commandCallback(cmd);
                    }

                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        log.WarnException(
                            "Failure to replicate changes to relational database for: " + cfg.Name + ", will continue trying." +
                            Environment.NewLine + cmd.CommandText, e);
                        replicationStatistics.RecordWriteError(e, database);
                        hadErrors = true;
                    }
                    finally
                    {
                        sp.Stop();

                        var elapsedMiliseconds = sp.ElapsedMilliseconds;

                        if (log.IsDebugEnabled)
                        {
                            log.Debug(string.Format("Delete took: {0}ms, statement: {1}", elapsedMiliseconds, stmt));
                        }

                        var elapsedMicroseconds = (long)(sp.ElapsedTicks * SystemTime.MicroSecPerTick);
                        replicationDeleteDurationHistogram.Update(elapsedMicroseconds);
                        replicationDeletesActionsHistogram.Update(1);
                        replicationDeletesActionsMetrics.Mark(1);

                        if (elapsedMiliseconds > LongStatementWarnThresholdInMiliseconds)
                        {
                            HandleSlowSql(elapsedMiliseconds, stmt);
                        }
                    }
                }
            }
        }

        private void HandleSlowSql(long elapsedMiliseconds, string stmt)
        {
            var message = string.Format("Slow SQL detected. Execution took: {0}ms, statement: {1}", elapsedMiliseconds, stmt);
            log.Warn(message);
            database.AddAlert(new Alert
            {
                AlertLevel = AlertLevel.Warning,
                CreatedAt = SystemTime.UtcNow,
                Message = message,
                Title = "Slow SQL statement",
                UniqueKey = "Slow SQL statement"
            });
        }

        private string GetTableNameString(string tableName)
        {
            if (cfg.QuoteTables)
            {
                return string.Join(".", tableName.Split('.').Select(x => commandBuilder.QuoteIdentifier(x)).ToArray());
            }
            else
            {
                return tableName;
            }
        }

        public static string SanitizeSqlValue(string sqlValue)
        {
            return sqlValue.Replace("'", "''");
        }

        private static string GetParameterName(DbProviderFactory providerFactory, DbCommandBuilder commandBuilder, string paramName)
        {
            switch (providerFactory.GetType().Name)
            {
                case "SqlClientFactory":
                case "MySqlClientFactory":
                    return "@" + paramName;

                case "OracleClientFactory":
                case "NpgsqlFactory":
                    return ":" + paramName;

                default:
                    // If we don't know, try to get it from the CommandBuilder.
                    return getParameterNameFromBuilder(commandBuilder, paramName);
            }
        }

        private static readonly Func<DbCommandBuilder, string, string> getParameterNameFromBuilder =
            (Func<DbCommandBuilder, string, string>)
            Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, string, string>),
                                    typeof(DbCommandBuilder).GetMethod("GetParameterName",
                                                                         BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder,
                                                                         new[] { typeof(string) }, null));


        public static void SetParamValue(DbParameter colParam, RavenJToken val, List<Func<DbParameter, String, Boolean>> stringParsers)
        {
            if (val == null)
                colParam.Value = DBNull.Value;
            else
            {
                switch (val.Type)
                {
                    case JTokenType.None:
                    case JTokenType.Uri:
                    case JTokenType.Raw:
                    case JTokenType.Array:
                        colParam.Value = val.Value<string>();
                        return;
                    case JTokenType.Object:
                        var objectValue = val as RavenJObject;
                        if (objectValue != null && objectValue.Keys.Count >= 2 && objectValue.ContainsKey("Type") && objectValue.ContainsKey("Value"))
                        {
                            var dbType = objectValue["Type"].Value<string>();
                            var fieldValue = objectValue["Value"].Value<string>();

                            colParam.DbType = (DbType)Enum.Parse(typeof(DbType), dbType, false);

                            colParam.Value = fieldValue;

                            if (objectValue.ContainsKey("Size"))
                            {
                                var size = objectValue["Size"].Value<int>();
                                colParam.Size = size;
                            }
                            return;
                        }
                        if (objectValue != null && objectValue.Keys.Count >= 4
                            && objectValue.ContainsKey("EnumType") && objectValue.ContainsKey("EnumValue")
                            && objectValue.ContainsKey("EnumProperty") && (objectValue.ContainsKey("Value") || objectValue.ContainsKey("Values")))
                        {
                            var enumType = Type.GetType(objectValue["EnumType"].Value<string>(), false);
                            if (enumType == null)
                            {
                                throw new InvalidOperationException(string.Format("Couldn't find type '{0}'.", objectValue["EnumType"]));
                            }
                            var enumStringvalue = objectValue["EnumValue"].Value<string>();
                            object enumValue;
                            if (enumStringvalue.Contains("|"))
                            {
                                var splitvalue = enumStringvalue.Split('|').Select(e => (int)Enum.Parse(enumType, e.Trim()));
                                enumValue = splitvalue.Aggregate((a, b) => a | b);
                            }
                            else
                            {
                                enumValue = Enum.Parse(enumType, enumStringvalue);
                            }

                            var property = colParam.GetType().GetProperty(objectValue["EnumProperty"].Value<string>());
                            if (property == null)
                            {
                                throw new InvalidOperationException(string.Format("Missing property '{0}' on type '{1}' of parameter.",
                                    objectValue["EnumProperty"], colParam.GetType().FullName));
                            }
                            if (objectValue.ContainsKey("Value"))
                            {
                                colParam.Value = objectValue["Value"].Value<object>();
                            }
                            else if (objectValue.ContainsKey("Values"))
                            {
                                colParam.Value = objectValue["Values"].Values<object>().ToArray();
                            }
                            property.SetValue(colParam, enumValue);

                            if (objectValue.ContainsKey("Size"))
                            {
                                var size = objectValue["Size"].Value<int>();
                                colParam.Size = size;
                            }
                            return;
                        }
                        else
                        {
                            colParam.Value = val.Value<string>();
                            return;
                        }

                    case JTokenType.String:
                        var value = val.Value<string>();
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
                        return;
                    case JTokenType.Integer:
                    case JTokenType.Date:
                    case JTokenType.Bytes:
                    case JTokenType.Guid:
                    case JTokenType.Boolean:
                    case JTokenType.TimeSpan:
                    case JTokenType.Float:
                        colParam.Value = val.Value<object>();
                        return;
                    case JTokenType.Null:
                    case JTokenType.Undefined:
                        colParam.Value = DBNull.Value;
                        return;
                    default:
                        throw new InvalidOperationException("Cannot understand how to save " + val.Type + " for " + colParam.ParameterName);
                }
            }
        }

        private DbProviderFactory GetDbProviderFactory(SqlReplicationConfig cfg)
        {
            DbProviderFactory providerFactory;
            try
            {
                providerFactory = DbProviderFactories.GetFactory(cfg.FactoryName);
            }
            catch (Exception e)
            {
                log.WarnException(
                    string.Format("Could not find provider factory {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
                                    cfg.Name), e);

                database.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = SystemTime.UtcNow,
                    Exception = e.ToString(),
                    Title = "Sql Replication could not find factory provider",
                    Message = string.Format("Could not find factory provider {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
                                    cfg.Name),
                    UniqueKey = string.Format("Sql Replication Provider Not Found: {0}, {1}", cfg.Name, cfg.FactoryName)
                });

                throw;
            }
            return providerFactory;
        }



        public void Dispose()
        {
            tx.Dispose();
            commandBuilder.Dispose();
            connection.Dispose();
        }
    }
}
