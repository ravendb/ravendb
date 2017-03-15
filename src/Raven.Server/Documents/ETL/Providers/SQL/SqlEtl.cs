using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Server.Documents.ETL.Providers.SQL.Connections;
using Raven.Server.Documents.ETL.Providers.SQL.Enumerators;
using Raven.Server.Documents.ETL.Providers.SQL.Metrics;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlEtl : EtlProcess<ToSqlItem, SqlTableWithRecords>
    {
        public const string SqlEtlTag = "SQL ETL";

        public SqlEtlConfiguration SqlConfiguration { get; }

        private PredefinedSqlConnection _predefinedSqlConnection;

        public readonly SqlEtlMetricsCountersManager Metrics = new SqlEtlMetricsCountersManager();

        public SqlEtl(DocumentDatabase database, SqlEtlConfiguration configuration) : base(database, configuration, SqlEtlTag)
        {
            SqlConfiguration = configuration;
        }

        protected override IEnumerator<ToSqlItem> ConvertDocsEnumerator(IEnumerator<Document> docs)
        {
            return new DocumentsToSqlItems(docs);
        }

        protected override IEnumerator<ToSqlItem> ConvertTombstonesEnumerator(IEnumerator<DocumentTombstone> tombstones)
        {
            return new TombstonesToSqlItems(tombstones);
        }

        public override IEnumerable<SqlTableWithRecords> Transform(IEnumerable<ToSqlItem> items, DocumentsOperationContext context)
        {
            var patcher = new SqlPatchDocument(Database, context, SqlConfiguration);

            foreach (var toSqlItem in items)
            {
                CancellationToken.ThrowIfCancellationRequested();

                Statistics.LastProcessedEtag = toSqlItem.Etag; // TODO arek

                try
                {
                    patcher.Transform(toSqlItem, context);

                    Statistics.TransformationSuccess();

                    if (CanContinueBatch() == false)
                        break;
                }
                catch (JavaScriptParseException e)
                {
                    var message = $"[{Name}] Could not parse transformation script. Stopping ETL process.";

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(message, e);

                    var alert = AlertRaised.Create(
                        Tag,
                        message,
                        AlertType.Etl_TransformationError,
                        NotificationSeverity.Error,
                        key: Name,
                        details: new ExceptionDetails(e));

                    Database.NotificationCenter.Add(alert);

                    Stop();

                    break;
                }
                catch (Exception e)
                {
                    Statistics.RecordTransformationError(e);

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Could not process SQL ETL script for '{Name}', skipping document: {toSqlItem.DocumentKey}", e);
                }
            }

            return patcher.Tables.Values;
        }

        public override bool Load(IEnumerable<SqlTableWithRecords> records)
        {
            var hadWork = false;

            try
            {
                using (var writer = new RelationalDatabaseWriter(this, _predefinedSqlConnection, Database))
                {
                    foreach (var table in records)
                    {
                        hadWork = true;

                        var stats = writer.Write(table, CancellationToken);

                        LogStats(stats, table);
                    }

                    writer.Commit();
                }
                
                Statistics.LoadSuccess(NumberOfExtractedItemsInCurrentBatch);
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failure to insert changes to relational database for '{Name}'", e);

                if (Statistics.LastErrorTime == null)
                    FallbackTime = TimeSpan.FromSeconds(5);
                else
                {
                    // double the fallback time (but don't cross 15 minutes)
                    var secondsSinceLastError = (Database.Time.GetUtcNow() - Statistics.LastErrorTime.Value).TotalSeconds;

                    FallbackTime = TimeSpan.FromSeconds(Math.Min(60 * 15, Math.Max(5, secondsSinceLastError * 2)));
                }

                Statistics.RecordLoadError(e, NumberOfExtractedItemsInCurrentBatch);
            }

            return hadWork;
        }

        private void LogStats(SqlWriteStats stats, SqlTableWithRecords table)
        {
            if (table.Inserts.Count > 0)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"[{Name}] Inserted {stats.InsertedRecordsCount} (out of {table.Inserts.Count}) records to '{table.TableName}' table " +
                        $"from the following documents: {string.Join(", ", table.Inserts.Select(x => x.DocumentKey))}");
                }
            }

            if (table.Deletes.Count > 0)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"[{Name}] Deleted {stats.DeletedRecordsCount} (out of {table.Deletes.Count}) records from '{table.TableName}' table " +
                        $"for the following documents: {string.Join(", ", table.Inserts.Select(x => x.DocumentKey))}");
                }
            }
        }

        public override bool CanContinueBatch()
        {
            return true; // TODO
        }

        public bool PrepareSqlEtlConfig(BlittableJsonReaderObject connections, bool writeToLog = true)
        {
            if (string.IsNullOrWhiteSpace(SqlConfiguration.ConnectionStringName) == false)
            {
                object connection;
                if (connections.TryGetMember(SqlConfiguration.ConnectionStringName, out connection))
                {
                    _predefinedSqlConnection = JsonDeserializationServer.PredefinedSqlConnection(connection as BlittableJsonReaderObject);
                    if (_predefinedSqlConnection != null)
                    {
                        return true;
                    }
                }

                var message =
                    $"Could not find connection string named '{SqlConfiguration.ConnectionStringName}' for SQL ETL config: " +
                    $"{SqlConfiguration.Name}, ignoring SQL ETL setting.";

                if (writeToLog)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info(message);
                }

                Statistics.LastAlert = AlertRaised.Create(Tag, message, AlertType.SqlEtl_ConnectionStringMissing, NotificationSeverity.Error);

                return false;
            }

            var emptyConnectionStringMsg =
                $"Connection string name cannot be empty for SQL ETL config: {SqlConfiguration.Name}, ignoring SQL ETL setting.";

            if (writeToLog)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info(emptyConnectionStringMsg);
            }

            Statistics.LastAlert = AlertRaised.Create(Tag, emptyConnectionStringMsg, AlertType.SqlEtl_ConnectionStringMissing, NotificationSeverity.Error);

            return false;
        }

        protected override void LoadLastProcessedEtag(DocumentsOperationContext context)
        {
            var sqlEtlStatus = Database.DocumentsStorage.Get(context, Constants.Documents.SqlReplication.RavenSqlReplicationStatusPrefix + Name);
            if (sqlEtlStatus == null)
            {
                Statistics.LastProcessedEtag = 0;
            }
            else
            {
                var etlStatus = JsonDeserializationServer.SqlEtlStatus(sqlEtlStatus.Data);
                Statistics.LastProcessedEtag = etlStatus.LastProcessedEtag;
            }
        }

        protected override void StoreLastProcessedEtag(DocumentsOperationContext context)
        {
            var key = Constants.Documents.SqlReplication.RavenSqlReplicationStatusPrefix + Name;
            var document = context.ReadObject(new DynamicJsonValue
            {
                [nameof(SqlEtlStatus.Name)] = Name,
                [nameof(SqlEtlStatus.LastProcessedEtag)] = Statistics.LastProcessedEtag
            }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            Database.DocumentsStorage.Put(context, key, null, document);
        }

        public bool ValidateName()
        {
            if (string.IsNullOrWhiteSpace(SqlConfiguration.Name) == false)
                return true;

            var message = $"Could not find name for SQL ETL document {SqlConfiguration.Name}, ignoring";

            if (Logger.IsInfoEnabled)
                Logger.Info(message);

            Statistics.LastAlert = AlertRaised.Create(Tag, message, AlertType.SqlEtl_ConnectionStringMissing, NotificationSeverity.Error);
            return false;
        }

        public DynamicJsonValue Simulate(SimulateSqlEtl simulateSqlEtl, DocumentsOperationContext context, IEnumerable<SqlTableWithRecords> toWrite)
        {
            if (simulateSqlEtl.PerformRolledBackTransaction)
            {
                var summaries = new List<TableQuerySummary>();

                using (var writer = new RelationalDatabaseWriter(this, _predefinedSqlConnection, Database))
                {
                    foreach (var records in toWrite)
                    {
                        var commands = new List<DbCommand>();

                        writer.Write(records, CancellationToken, commands);

                        summaries.Add(TableQuerySummary.GenerateSummaryFromCommands(records.TableName, commands));
                    }

                    writer.Rollback();
                }

                return new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(summaries.ToArray()),
                    ["LastAlert"] = Statistics.LastAlert,
                };
            }
            else
            {
                var tableQuerySummaries = new List<TableQuerySummary.CommandData>();

                var simulatedwriter = new RelationalDatabaseWriterSimulator(_predefinedSqlConnection, SqlConfiguration);

                foreach (var records in toWrite)
                {
                    var commands = simulatedwriter.SimulateExecuteCommandText(records, CancellationToken);

                    tableQuerySummaries.AddRange(commands.Select(x => new TableQuerySummary.CommandData
                    {
                        CommandText = x
                    }));
                }

                return new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(tableQuerySummaries.ToArray()),
                    ["LastAlert"] = Statistics.LastAlert,
                };
            }
        }

        public static DynamicJsonValue SimulateSqlEtl(SimulateSqlEtl simulateSqlEtl, DocumentDatabase database, DocumentsOperationContext context)
        {
            try
            {
                var document = database.DocumentsStorage.Get(context, simulateSqlEtl.DocumentId);

                using (var etl = new SqlEtl(database, simulateSqlEtl.Configuration))
                {
                    // TODO arek
                    //if (etl.PrepareSqlEtlConfig(_connections, false) == false)
                    //{
                    //    return new DynamicJsonValue
                    //    {
                    //        ["LastAlert"] = etl.Statistics.LastAlert,
                    //    };
                    //}
                    
                    var transformed = etl.Transform(new[] { new ToSqlItem(document) }, context);

                    return etl.Simulate(simulateSqlEtl, context, transformed);
                }
            }
            catch (Exception e)
            {
                return new DynamicJsonValue
                {
                    ["LastAlert"] =
                    AlertRaised.Create("SQL ETL",
                        $"Simulate SQL ETL operation for {simulateSqlEtl.Configuration.Name} failed",
                        AlertType.Etl_Error,
                        NotificationSeverity.Error,
                        key: simulateSqlEtl.Configuration.Name,
                        details: new ExceptionDetails(e)).ToJson()
                };
            }
        }

        protected override void UpdateMetrics(DateTime startTime, Stopwatch duration, int batchSize)
        {
            Metrics.BatchSizeMeter.Mark(batchSize);

            Metrics.UpdateReplicationPerformance(new SqlEtlPerformanceStats
            {
                BatchSize = batchSize,
                Duration = duration.Elapsed,
                Started = startTime
            });
        }
    }
}