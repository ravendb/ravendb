using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.SqlReplication
{
    public class RelationalDatabaseWriter : RelationalDatabaseWriterBase, IDisposable
    {
        private readonly ILog log = LogManager.GetLogger(typeof(RelationalDatabaseWriter));

        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly SqlReplicationConfiguration _configuration;
        private readonly PredefinedSqlConnection _predefinedSqlConnection;
        private readonly SqlReplicationStatistics _statistics;
        private readonly CancellationToken _cancellationToken;

        private readonly DbCommandBuilder _commandBuilder;
        private readonly DbProviderFactory _providerFactory;
        private readonly DbConnection _connection;
        private readonly DbTransaction _tx;

        private readonly List<Func<DbParameter, String, Boolean>> stringParserList;

        private const int LongStatementWarnThresholdInMilliseconds = 3000;

        bool hadErrors;

        public RelationalDatabaseWriter(DocumentDatabase database, DocumentsOperationContext context, SqlReplicationConfiguration configuration,
            PredefinedSqlConnection predefinedSqlConnection, SqlReplicationStatistics statistics, CancellationToken cancellationToken) 
            : base(predefinedSqlConnection)
        {
            _database = database;
            _context = context;
            _configuration = configuration;
            _predefinedSqlConnection = predefinedSqlConnection;
            _statistics = statistics;
            _cancellationToken = cancellationToken;

            _providerFactory = GetDbProviderFactory(configuration);
            _commandBuilder = _providerFactory.CreateCommandBuilder();
            _connection = _providerFactory.CreateConnection();
            _connection.ConnectionString = predefinedSqlConnection.ConnectionString;

            try
            {
                _connection.Open();
            }
            catch (Exception e)
            {
                database.AddAlert(new Alert
                {
                    IsError = true,
                    CreatedAt = SystemTime.UtcNow,
                    Exception = e.ToString(),
                    Title = "Sql Replication could not open connection",
                    Message = "Sql Replication could not open connection to " + _connection.ConnectionString,
                    UniqueKey = "Sql Replication Connection Error: " + _connection.ConnectionString,
                });
                throw;
            }

            _tx = _connection.BeginTransaction();

            stringParserList = GenerateStringParsers();
            /* sqlReplicationMetrics = database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault().GetSqlReplicationMetricsManager(cfg);*/
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

        private DbProviderFactory GetDbProviderFactory(SqlReplicationConfiguration configuration)
        {
            DbProviderFactory providerFactory;
            try
            {
                providerFactory = DbProviderFactories.GetFactory(_predefinedSqlConnection.FactoryName);
            }
            catch (Exception e)
            {
                log.WarnException($"Could not find provider factory {_predefinedSqlConnection.FactoryName} to replicate to sql for {configuration.Name}, ignoring", e);

                _database.AddAlert(new Alert
                {
                    IsError = true,
                    CreatedAt = SystemTime.UtcNow,
                    Exception = e.ToString(),
                    Title = "Sql Replication could not find factory provider",
                    Message = $"Could not find factory provider {_predefinedSqlConnection.FactoryName} to replicate to sql for {configuration.Name}, ignoring",
                    UniqueKey = $"Sql Replication Provider Not Found: {configuration.Name}, {_predefinedSqlConnection.FactoryName}",
                });

                throw;
            }
            return providerFactory;
        }

        public void Dispose()
        {
            _tx.Dispose();
            _connection.Dispose();
        }

        public bool Commit()
        {
            _tx.Commit();
            return true;
        }

        private void InsertItems(string tableName, string pkName, List<ItemToReplicate> dataForTable, Action<DbCommand> commandCallback = null)
        {
            /*var sqlReplicationTableMetrics = sqlReplicationMetrics.GetTableMetrics(tableName);
            var replicationInsertActionsMetrics = sqlReplicationTableMetrics.SqlReplicationInsertActionsMeter;
            var replicationInsertActionsHistogram = sqlReplicationTableMetrics.SqlReplicationInsertActionsHistogram;
            var replicationInsertDurationHistogram = sqlReplicationTableMetrics.SqlReplicationInsertActionsDurationHistogram;*/

            var sp = new Stopwatch();
            foreach (var itemToReplicate in dataForTable)
            {
                sp.Restart();
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.Transaction = _tx;

/*TODO: Should I throw here or return */
                    _cancellationToken.ThrowIfCancellationRequested();

                    var sb = new StringBuilder("INSERT INTO ")
                        .Append(GetTableNameString(tableName))
                        .Append(" (")
                        .Append(_commandBuilder.QuoteIdentifier(pkName))
                        .Append(", ");
                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Key == pkName)
                            continue;
                        sb.Append(_commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;

                    var pkParam = cmd.CreateParameter();

                    pkParam.ParameterName = GetParameterName(pkName);
                    pkParam.Value = itemToReplicate.DocumentKey;
                    cmd.Parameters.Add(pkParam);

                    sb.Append(") \r\nVALUES (")
                        .Append(GetParameterName(pkName))
                        .Append(", ");

                    foreach (var column in itemToReplicate.Columns)
                    {
                        if (column.Key == pkName)
                            continue;
                        var colParam = cmd.CreateParameter();
                        colParam.ParameterName = column.Key;
                        SetParamValue(colParam, column, stringParserList);
                        cmd.Parameters.Add(colParam);
                        sb.Append(GetParameterName(column.Key)).Append(", ");
                    }
                    sb.Length = sb.Length - 2;
                    sb.Append(")");

                    if (IsSqlServerFactoryType && _configuration.ForceSqlServerQueryRecompile)
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
                            "Failure to replicate changes to relational database for: " + _configuration.Name + " (doc: " + itemToReplicate.DocumentKey + " ), will continue trying." +
                            Environment.NewLine + cmd.CommandText, e);
                        _statistics.RecordWriteError(e, _database);
                        hadErrors = true;
                    }
                    finally
                    {
                        sp.Stop();

                        var elapsedMilliseconds = sp.ElapsedMilliseconds;

                        if (log.IsDebugEnabled)
                        {
                            log.Debug($"Insert took: {elapsedMilliseconds}ms, statement: {stmt}");
                        }

                        var elapsedMicroseconds = (long)(sp.ElapsedTicks * SystemTime.MicroSecPerTick);
                       /* replicationInsertDurationHistogram.Update(elapsedMicroseconds);
                        replicationInsertActionsMetrics.Mark(1);
                        replicationInsertActionsHistogram.Update(1);*/

                        if (elapsedMilliseconds > LongStatementWarnThresholdInMilliseconds)
                        {
                            HandleSlowSql(elapsedMilliseconds, stmt);
                        }
                    }
                }
            }
        }

        public void DeleteItems(string tableName, string pkName, bool doNotParameterize, List<string> documentKeys, Action<DbCommand> commandCallback = null)
        {
            const int maxParams = 1000;
            /*var sqlReplicationTableMetrics = sqlReplicationMetrics.GetTableMetrics(tableName);
            var replicationDeleteDurationHistogram = sqlReplicationTableMetrics.SqlReplicationDeleteActionsDurationHistogram;
            var replicationDeletesActionsMetrics = sqlReplicationTableMetrics.SqlReplicationDeleteActionsMeter;
            var replicationDeletesActionsHistogram = sqlReplicationTableMetrics.SqlReplicationDeleteActionsHistogram;*/

            var sp = new Stopwatch();
            using (var cmd = _connection.CreateCommand())
            {
                sp.Start();
                cmd.Transaction = _tx;
                _cancellationToken.ThrowIfCancellationRequested(); // TODO: Should i throw or return
                for (int i = 0; i < documentKeys.Count; i += maxParams)
                {
                    cmd.Parameters.Clear();
                    var sb = new StringBuilder("DELETE FROM ")
                        .Append(GetTableNameString(tableName))
                        .Append(" WHERE ")
                        .Append(_commandBuilder.QuoteIdentifier(pkName))
                        .Append(" IN (");

                    for (int j = i; j < Math.Min(i + maxParams, documentKeys.Count); j++)
                    {
                        if (i != j)
                            sb.Append(", ");
                        if (doNotParameterize == false)
                        {
                            var dbParameter = cmd.CreateParameter();
                            dbParameter.ParameterName = GetParameterName("p" + j);
                            dbParameter.Value = documentKeys[j];
                            cmd.Parameters.Add(dbParameter);
                            sb.Append(dbParameter.ParameterName);
                        }
                        else
                        {
                            sb.Append("'").Append(SanitizeSqlValue(documentKeys[j])).Append("'");
                        }
                    }
                    sb.Append(")");

                    if (IsSqlServerFactoryType && _configuration.ForceSqlServerQueryRecompile)
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
                        log.WarnException("Failure to replicate changes to relational database for: " + _configuration.Name + ", will continue trying." + Environment.NewLine + cmd.CommandText, e);
                        _statistics.RecordWriteError(e, _database);
                        hadErrors = true;
                    }
                    finally     
                    {
                        sp.Stop();

                        var elapsedMiliseconds = sp.ElapsedMilliseconds;

                        if (log.IsDebugEnabled)
                        {
                            log.Debug($"Delete took: {elapsedMiliseconds}ms, statement: {stmt}");
                        }

                        var elapsedMicroseconds = (long)(sp.ElapsedTicks * SystemTime.MicroSecPerTick);
                      /*  replicationDeleteDurationHistogram.Update(elapsedMicroseconds);
                        replicationDeletesActionsHistogram.Update(1);
                        replicationDeletesActionsMetrics.Mark(1);*/

                        if (elapsedMiliseconds > LongStatementWarnThresholdInMilliseconds)
                        {
                            HandleSlowSql(elapsedMiliseconds, stmt);
                        }
                    }
                }
            }
        }

        private void HandleSlowSql(long elapsedMiliseconds, string stmt)
        {
            var message = $"Slow SQL detected. Execution took: {elapsedMiliseconds}ms, statement: {stmt}";
            log.Warn(message);
            _database.AddAlert(new Alert
            {
                IsError = false,
                CreatedAt = SystemTime.UtcNow,
                Message = message,
                Title = "Slow SQL statement",
                UniqueKey = "Slow SQL statement"
            });
        }

        private string GetTableNameString(string tableName)
        {
            if (_configuration.QuoteTables)
            {
                return string.Join(".", tableName.Split('.').Select(_commandBuilder.QuoteIdentifier).ToArray());
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

        private string GetParameterName(string paramName)
        {
            switch (_providerFactory.GetType().Name)
            {
                case "SqlClientFactory":
                case "MySqlClientFactory":
                    return "@" + paramName;

                case "OracleClientFactory":
                case "NpgsqlFactory":
                    return ":" + paramName;

                default:
                    throw new NotImplementedException();
            }
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

        public bool ExecuteScript(SqlReplicationScriptResult scriptResult)
        {
            foreach (var sqlReplicationTable in _configuration.SqlReplicationTables)
            {
                if (sqlReplicationTable.InsertOnlyMode)
                    continue;

                // first, delete all the rows that might already exist there
                DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, _configuration.ParameterizeDeletesDisabled, scriptResult.Keys);
            }
/* TODO: we might want to join these foreach*/
            foreach (var sqlReplicationTable in _configuration.SqlReplicationTables)
            {
                List<ItemToReplicate> dataForTable;
                if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
                    continue;

                InsertItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable);
            }

            Commit();

            return hadErrors == false;
        }

        public IEnumerable<TableQuerySummary> RolledBackExecute(SqlReplicationScriptResult scriptResult)
        {
            var identifiers = scriptResult.Data.SelectMany(x => x.Value).Select(x => x.DocumentKey).Distinct().ToList();

            // first, delete all the rows that might already exist there
            foreach (var sqlReplicationTable in _configuration.SqlReplicationTables)
            {
                var commands = new List<DbCommand>();
                DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, _configuration.ParameterizeDeletesDisabled,
                    identifiers, commands.Add);
                yield return TableQuerySummary.GenerateSummaryFromCommands(sqlReplicationTable.TableName, commands);
            }
            /* TODO: we might want to join these foreach*/

            foreach (var sqlReplicationTable in _configuration.SqlReplicationTables)
            {
                List<ItemToReplicate> dataForTable;
                if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
                    continue;
                var commands = new List<DbCommand>();
                InsertItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable, commands.Add);

                yield return TableQuerySummary.GenerateSummaryFromCommands(sqlReplicationTable.TableName, commands);
            }

            _tx.Rollback();
        }

        public static void SetParamValue(DbParameter colParam, SqlReplicationColumn column, List<Func<DbParameter, string, bool>> stringParsers)
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
                    case BlittableJsonToken.Float:
                        colParam.Value = column.Value;
                        break;

                    case BlittableJsonToken.String:
                        SetParamStringValue(colParam, ((LazyStringValue) column.Value).ToString(), stringParsers);
                        break;
                    case BlittableJsonToken.CompressedString:
                        SetParamStringValue(colParam, ((LazyCompressedStringValue) column.Value).ToString(), stringParsers);
                        break;

                    case BlittableJsonToken.StartObject:
                        var objectValue = (BlittableJsonReaderObject) column.Value;
                        if (objectValue.Count >= 2)
                        {
                            object dbType, fieldValue;
                            if (objectValue.TryGetMember("Type", out dbType) && objectValue.TryGetMember("Value", out fieldValue))
                            {
                                colParam.DbType = (DbType) Enum.Parse(typeof (DbType), dbType.ToString(), false);
                                colParam.Value = fieldValue.ToString();

                                object size;
                                if (objectValue.TryGetMember("Size", out size))
                                {
                                    colParam.Size = (int) size;
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
                        throw new InvalidOperationException("Cannot understand how to save " + column.Type + " for " + colParam.ParameterName);
                }
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
            return new List<Func<DbParameter, string, bool>> {
                (colParam, value) => {
                    if( char.IsDigit( value[ 0 ] ) ) {
                            DateTime dateTime;
                            if (DateTime.TryParseExact(value, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
                            {
                                switch(_providerFactory.GetType( ).Name ) {
                                    case "MySqlClientFactory":
                                        colParam.Value = dateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
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
                            switch( _providerFactory.GetType( ).Name ) {
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
    }
}