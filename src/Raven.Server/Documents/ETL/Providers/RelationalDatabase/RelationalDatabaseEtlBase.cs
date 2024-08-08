using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Enumerators;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Metrics;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase;

public abstract class RelationalDatabaseEtlBase<TRelationalEtlConfiguration, TRelationalConnectionString>
    : EtlProcess<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, TRelationalEtlConfiguration, TRelationalConnectionString, EtlStatsScope, EtlPerformanceOperation>
    where TRelationalConnectionString : ConnectionString
    where TRelationalEtlConfiguration : EtlConfiguration<TRelationalConnectionString>
{
    public readonly RelationalDatabaseEtlMetricsCountersManager RelationalMetrics = new();

    public RelationalDatabaseEtlBase(Transformation transformation, TRelationalEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore,
        string relationalEtlTag)
        : base(transformation, configuration, database, serverStore, relationalEtlTag)
    {
        Metrics = RelationalMetrics;
    }

    public abstract override EtlType EtlType { get; }
    
    protected override IEnumerator<ToRelationalDatabaseItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToRelationalDatabaseItems(docs, collection);
    }
    
    protected override IEnumerator<ToRelationalDatabaseItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        return new TombstonesToRelationalDatabaseItems(tombstones, collection);
    }
    
    protected override IEnumerator<ToRelationalDatabaseItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"Attachment tombstones aren't supported by {Configuration.EtlType.ToString()} ETL");
    }

    protected override IEnumerator<ToRelationalDatabaseItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"Counters aren't supported by {Configuration.EtlType.ToString()} ETL");
    }

    protected override IEnumerator<ToRelationalDatabaseItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"Time series aren't supported by {Configuration.EtlType.ToString()} ETL");
    }

    protected override IEnumerator<ToRelationalDatabaseItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"Time series aren't supported by {Configuration.EtlType.ToString()} ETL");
    }

    protected override bool ShouldTrackAttachmentTombstones()
    {
        return false;
    }

    public override bool ShouldTrackCounters() => false;

    public override bool ShouldTrackTimeSeries() => false;

    protected abstract override EtlTransformer<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(
        DocumentsOperationContext context);

    protected override int LoadInternal(IEnumerable<RelationalDatabaseTableWithRecords> records, DocumentsOperationContext context, EtlStatsScope scope)
    {
        var count = 0;

        using (var lazyWriter =
               new DisposableLazy<RelationalDatabaseWriterBase<TRelationalConnectionString, TRelationalEtlConfiguration>>(
                   GetRelationalDatabaseWriterInstance))
        {
            foreach (var table in records)
            {
                var writer = lazyWriter.Value;

                var stats = writer.Write(table, null, CancellationToken);

                LogStats(stats, table);

                count += stats.DeletedRecordsCount + stats.InsertedRecordsCount;
            }

            if (lazyWriter.IsValueCreated)
            {
                lazyWriter.Value.Commit();
            }
        }

        return count;
    }

    protected abstract RelationalDatabaseWriterBase<TRelationalConnectionString, TRelationalEtlConfiguration> GetRelationalDatabaseWriterInstance();
    
    private void LogStats(RelationalDatabaseWriteStats stats, RelationalDatabaseTableWithRecords table)
    {
        if (table.Inserts.Count > 0)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"[{Name}] Inserted {stats.InsertedRecordsCount} (out of {table.Inserts.Count}) records to '{table.TableName}' table " +
                            $"from the following documents: {string.Join(", ", table.Inserts.Select(x => x.DocumentId))}");
            }
        }

        if (table.Deletes.Count > 0)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"[{Name}] Deleted {stats.DeletedRecordsCount} (out of {table.Deletes.Count}) records from '{table.TableName}' table " +
                            $"for the following documents: {string.Join(", ", table.Inserts.Select(x => x.DocumentId))}");
            }
        }
    }

    protected override EtlStatsScope CreateScope(EtlRunStats stats)
    {
        return new EtlStatsScope(stats);
    }

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    protected abstract RelationalDatabaseWriterSimulator GetWriterSimulator();

    public RelationalDatabaseEtlTestScriptResult RunTest(DocumentsOperationContext context, IEnumerable<RelationalDatabaseTableWithRecords> toWrite, bool performRolledBackTransaction)
    {
        var summaries = new List<TableQuerySummary>();

        if (performRolledBackTransaction)
        {
            try
            {
                using (var writer = GetRelationalDatabaseWriterInstance())
                {
                    foreach (var records in toWrite)
                    {
                        var commands = new List<DbCommand>();

                        writer.Write(records, commands, CancellationToken);

                        summaries.Add(TableQuerySummary.GenerateSummaryFromCommands(records.TableName, commands));
                    }

                    writer.Rollback();
                }
            }
            catch (Exception e)
            {
                Statistics.RecordPartialLoadError(e.ToString(), documentId: null, count: 1);
            }
        }
        else
        {
            var simulatedWriter = GetWriterSimulator();

            foreach (var records in toWrite)
            {
                var commands = simulatedWriter.SimulateExecuteCommandText(records, CancellationToken).Select(x => new TableQuerySummary.CommandData { CommandText = x })
                    .ToArray();

                summaries.Add(new TableQuerySummary { TableName = records.TableName, Commands = commands });
            }
        }

        return new RelationalDatabaseEtlTestScriptResult
        {
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
            LoadErrors = Statistics.LastLoadErrorsInCurrentBatch.Errors.ToList(),
            SlowSqlWarnings = Statistics.LastSlowSqlWarningsInCurrentBatch.Statements.ToList(),
            Summary = summaries
        };
    }
}
