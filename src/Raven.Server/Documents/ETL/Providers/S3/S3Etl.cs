using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    public class S3Etl : EtlProcess<ToS3Item, RowGroups, S3EtlConfiguration, S3ConnectionString>
    {
        public const string S3EtlTag = "S3 ETL";

        public readonly S3EtlMetricsCountersManager S3Metrics = new S3EtlMetricsCountersManager();

        private Timer _timer;

        public S3Etl(Transformation transformation, S3EtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, S3EtlTag)
        {
            Metrics = S3Metrics;

            _timer = new Timer(_ => _waitForChanges.Set(), null, TimeSpan.Zero, configuration.ETLFrequency);
        }

        public override EtlType EtlType => EtlType.S3;

        private static readonly IEnumerator<ToS3Item> EmptyEnumerator = Enumerable.Empty<ToS3Item>().GetEnumerator();

        protected override IEnumerator<ToS3Item> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToS3Items(docs, collection);
        }

        protected override IEnumerator<ToS3Item> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
        {
            return EmptyEnumerator; // todo
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
            throw new NotImplementedException();
        }

        protected override IEnumerator<ToS3Item> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series deletes aren't supported by S3 ETL");
        }

        public override void NotifyAboutWork(DatabaseChange change)
        {
            // intentionally not setting _waitForChanges here
            // _waitForChanges is being set by the timer
        }

        protected override bool ShouldTrackAttachmentTombstones()
        {
            return false;
        }

        protected override bool ShouldFilterOutHiLoDocument()
        {
            return true;
        }

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override EtlTransformer<ToS3Item, RowGroups> GetTransformer(DocumentsOperationContext context)
        {
            return new S3DocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<RowGroups> records, DocumentsOperationContext context)
        {
            var count = 0;

            foreach (var rowGroups in records)
            {
                var path = GetTempPath();

                // todo split to multiple files if needed
                using (Stream fileStream = File.OpenWrite(path))
                {
                    var fields = rowGroups.Fields;

                    using (var parquetWriter = new ParquetWriter(new Schema(fields), fileStream))
                    {
                        foreach (var rowGroup in rowGroups.Groups)
                        {
                            // create a new row group in the file
                            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                            {
                                for (var index = 0; index < rowGroup.Data.Length; index++)
                                {
                                    var data = rowGroup.Data[index];
                                    var field = (DataField)fields[index];
                                    Array array = default;

                                    // todo handle more types
                                    switch (field.DataType)
                                    {
                                        case DataType.Unspecified:
                                            break;
                                        case DataType.Boolean:
                                            array = ((List<bool>)data).ToArray();
                                            break;
                                        case DataType.Int32:
                                        case DataType.Int64:
                                            array = ((List<long>)data).ToArray();
                                            break;
                                        case DataType.String:
                                            array = ((List<string>)data).ToArray();
                                            break;
                                        case DataType.Float:
                                        case DataType.Double:
                                        case DataType.Decimal:
                                            array = ((List<double>)data).ToArray();
                                            break;
                                        default:
                                            throw new ArgumentOutOfRangeException();
                                    }

                                    groupWriter.WriteColumn(new DataColumn(field, array));

                                    count += data.Count;

                                    // todo log stats
                                }
                            }
                        }
                    }
                }

                // todo upload parquet file to s3/bucketName/TableName/PartitionKey/ and delete file from disc 
            }

            return count;
        }

        private string GetTempPath()
        {
            var dir = Configuration.TempDirectoryPath ?? Path.GetTempPath();
            var fileName = $"{Guid.NewGuid()}.parquet"; 

            return Path.Combine(dir, fileName);
        }
    }
}
