using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.Documents.ETL.Providers.SQL.Connections;
using Raven.Server.Documents.ETL.Providers.SQL.Enumerators;
using Raven.Server.Documents.ETL.Providers.SQL.Metrics;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
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

        private readonly PredefinedSqlConnection _predefinedSqlConnection;

        public readonly SqlEtlMetricsCountersManager SqlMetrics = new SqlEtlMetricsCountersManager();

        public SqlEtl(DocumentDatabase database, SqlEtlConfiguration configuration, PredefinedSqlConnection predefinedConnection)
            : base(database, configuration, SqlEtlTag)
        {
            _predefinedSqlConnection = predefinedConnection;
            SqlConfiguration = configuration;
            Metrics = SqlMetrics;
        }

        protected override IEnumerator<ToSqlItem> ConvertDocsEnumerator(IEnumerator<Document> docs)
        {
            return new DocumentsToSqlItems(docs);
        }

        protected override IEnumerator<ToSqlItem> ConvertTombstonesEnumerator(IEnumerator<DocumentTombstone> tombstones)
        {
            return new TombstonesToSqlItems(tombstones);
        }

        protected override EtlTransformer<ToSqlItem, SqlTableWithRecords> GetTransformer(DocumentsOperationContext context)
        {
            return new SqlDocumentTransformer(Database, context, SqlConfiguration);
        }

        protected override void LoadInternal(IEnumerable<SqlTableWithRecords> records, JsonOperationContext context)
        {
            using (var writer = new RelationalDatabaseWriter(this, _predefinedSqlConnection, Database))
            {
                foreach (var table in records)
                {
                    var stats = writer.Write(table, CancellationToken);

                    LogStats(stats, table);
                }

                writer.Commit();
            }
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

        protected override void HandleFallback()
        {
            if (Statistics.LastErrorTime == null)
                FallbackTime = TimeSpan.FromSeconds(5);
            else
            {
                // double the fallback time (but don't cross 15 minutes)
                var secondsSinceLastError = (Database.Time.GetUtcNow() - Statistics.LastErrorTime.Value).TotalSeconds;

                FallbackTime = TimeSpan.FromSeconds(Math.Min(60 * 15, Math.Max(5, secondsSinceLastError * 2)));
            }
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

                PredefinedSqlConnection predefinedConnection;
                var connectionStringName = simulateSqlEtl.Configuration.ConnectionStringName;

                if (database.EtlLoader.CurrentConfiguration.SqlConnections.TryGetValue(connectionStringName, out predefinedConnection) == false)
                {
                    throw new InvalidOperationException($"Could not find connection string named '{connectionStringName}' in ETL config");
                }

                using (var etl = new SqlEtl(database, simulateSqlEtl.Configuration, predefinedConnection))
                {
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
    }
}