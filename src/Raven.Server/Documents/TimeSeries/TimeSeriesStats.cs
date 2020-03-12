using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesStats
    {
        private static readonly Slice TimeSeriesStatsKey;
        private static readonly Slice PolicyIndex;
        private static readonly TableSchema TimeSeriesStatsSchema = new TableSchema();

        private enum StatsColumn
        {
            Key = 0, // documentId, separator, name
            PolicyName = 1,
            Count = 2,
           // Etag = 5 
        }

        static TimeSeriesStats()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "TimeSeriesStats", ByteStringType.Immutable, out TimeSeriesStatsKey);
                Slice.From(ctx, "PolicyIndex", ByteStringType.Immutable, out PolicyIndex);
            }

            TimeSeriesStatsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumn.Key,
                Count = 1, 
                Name = TimeSeriesStatsKey,
                IsGlobal = true
            });

            TimeSeriesStatsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumn.PolicyName,
                Name = PolicyIndex
            });
        }

        public TimeSeriesStats(Transaction tx)
        {
            tx.CreateTree(TimeSeriesStatsKey);
        }

        private Table GetOrCreateTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.TimeSeriesStats); // TODO: cache the collection and pass Slice
            TimeSeriesStatsSchema.Create(tx, tableName, 16);
            return tx.OpenTable(TimeSeriesStatsSchema, tableName);
        }

        public void UpdateCount(DocumentsOperationContext context, string docId, string name, CollectionName collection, long count)
        {
            using (var slicer = new TimeSeriesStorage.TimeSeriesSlicer(context, docId, name, default))
            {
                UpdateCount(context, slicer, collection, count);
            }
        }

        private void UpdateCount(DocumentsOperationContext context, TimeSeriesStorage.TimeSeriesSlicer slicer, CollectionName collection, long count)
        {
            long previousCount = 0;
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            if (table.ReadByKey(slicer.StatsKey, out var tvr))
            {
                previousCount = DocumentsStorage.TableValueToLong((int)StatsColumn.Count, ref tvr);
            }

            using (table.Allocate(out var tvb))
            {
                tvb.Add(slicer.StatsKey);
                tvb.Add(GetPolicy(slicer));
                tvb.Add(previousCount + count);
                //tvb.Add(etag)

                table.Set(tvb);
            }
        }

        public long GetCount(DocumentsOperationContext context, string docId, string name)
        {
            var table = new Table(TimeSeriesStatsSchema, context.Transaction.InnerTransaction);
            using (var slicer = new TimeSeriesStorage.TimeSeriesSlicer(context, docId, name, default))
            {
                return table.ReadByKey(slicer.StatsKey, out var tvr) ? DocumentsStorage.TableValueToLong((int)StatsColumn.Count, ref tvr) : 0;
            }
        }

        private Slice GetPolicy(TimeSeriesStorage.TimeSeriesSlicer slicer)
        {
            var name = slicer.LowerTimeSeriesName;
            var index = name.Content.IndexOf((byte)TimeSeriesConfiguration.TimeSeriesRollupSeparator);
            if (index < 0)
                return TimeSeriesPolicyRunner.RawPolicySlice;
            var offset = index + 1;
            return slicer.External(name, offset, name.Content.Length - offset);
        }
    }
}
