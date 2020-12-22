using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.Metrics;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    public class S3Etl : EtlProcess<ToS3Item, RowGroup, SqlEtlConfiguration, SqlConnectionString>
    {
        public const string S3EtlTag = "S3 ETL";

        public readonly SqlEtlMetricsCountersManager SqlMetrics = new SqlEtlMetricsCountersManager();


        public S3Etl(Transformation transformation, SqlEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, S3EtlTag)
        {
            Metrics = SqlMetrics;
        }

        public override EtlType EtlType => EtlType.S3;

        private readonly TimeSpan _batchFrequency = TimeSpan.FromMinutes(1);  // todo

        private long _lastBatchTime; // todo 

        private const long MinTimeToWait = 10_000_000;

        private const int MaxTimeToWait = int.MaxValue;


        private static IEnumerator<ToS3Item> _emptyEnumerator = Enumerable.Empty<ToS3Item>().GetEnumerator();

        protected override IEnumerator<ToS3Item> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToS3Items(docs, collection);
        }

        protected override IEnumerator<ToS3Item> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
        {
            return _emptyEnumerator; // todo
            throw new NotSupportedException("Tombstones aren't supported by S3 ETL");
        }

        protected override IEnumerator<ToS3Item> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by S3 ETL");
        }

        protected override IEnumerator<ToS3Item> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
        {
            throw new NotSupportedException("Counters aren't supported by S3 ETL");
        }

        protected override IEnumerator<ToS3Item> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
        {
            // todo
            throw new NotSupportedException("Time series aren't supported by S3 ETL");
        }

        protected override IEnumerator<ToS3Item> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series aren't supported by S3 ETL");
        }


        protected override bool ShouldTrackAttachmentTombstones()
        {
            return false;
        }

        protected override bool ShouldWait(out int ticks)
        {
            ticks = default;
            if (_lastBatchTime == 0)
                return false;
            
            var now = Database.Time.GetUtcNow().Ticks;
            var timeSinceLastBatch = now - _lastBatchTime;
            var timeToWait = timeSinceLastBatch - _batchFrequency.Ticks;

            if (timeToWait < MinTimeToWait) 
                return false;

            if (timeToWait > MaxTimeToWait)
                timeToWait = MaxTimeToWait;

            ticks = (int)timeToWait;
            return true;
        }

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override EtlTransformer<ToS3Item, RowGroup> GetTransformer(DocumentsOperationContext context)
        {
            return new S3DocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<RowGroup> records, DocumentsOperationContext context)
        {
            var count = 0;
            var fileNum = 0;

            foreach (var rowGroup in records)
            {
                using (Stream fileStream = File.OpenWrite($"d:\\work\\test{fileNum++}.parquet"))
                {
                    var fields = rowGroup.Fields;

                    using (var parquetWriter = new ParquetWriter(new Schema(fields), fileStream))
                    {
                        // create a new row group in the file
                        using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                        {
                            var groupData = rowGroup.Data;

                            for (var index = 0; index < groupData.Length; index++)
                            {
                                var data = groupData[index];
                                var field = (DataField)fields[index];

                                DataColumn dataColumn = default;
                                switch (field.DataType)
                                {
                                    case DataType.Unspecified:
                                        break;
                                    case DataType.Boolean:
                                        dataColumn = new DataColumn(field, ((List<bool>)data).ToArray());
                                        break;
                                    case DataType.Int32:
                                    case DataType.Int64:
                                        dataColumn = new DataColumn(field, ((List<long>)data).ToArray());
                                        break;
                                    case DataType.String:
                                        dataColumn = new DataColumn(field, ((List<string>)data).ToArray());
                                        break;
                                    case DataType.Float:
                                    case DataType.Double:
                                    case DataType.Decimal:
                                        dataColumn = new DataColumn(field, ((List<double>)data).ToArray());
                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException();
                                }

                                groupWriter.WriteColumn(dataColumn);
                            }
                               

                            count += groupData[0].Count;
                        }
                    }
                }
            }

            //using (var parquet = new DisposableLazy<RelationalDatabaseWriter>(() => new RelationalDatabaseWriter(this, Database)))
/*                foreach (var table in records)
                {
                    var writer = lazyWriter.Value;

                    var stats = writer.Write(table, null, CancellationToken);


                    LogStats(stats, table);

                    count += stats.DeletedRecordsCount + stats.InsertedRecordsCount;
                }

                if (lazyWriter.IsValueCreated)
                {
                    lazyWriter.Value.Commit();
                }*/

            _lastBatchTime = Database.Time.GetUtcNow().Ticks;

            return count;
        }

        protected override bool ShouldFilterOutHiLoDocument()
        {
            return true;
        }
    }
}
