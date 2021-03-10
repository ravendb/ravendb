using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlaptEtl : EtlProcess<ToOlapItem, RowGroups, OlapEtlConfiguration, OlapEtlConnectionString>
    {
        public const string OlaptEtlTag = "OLAP ETL";

        public readonly OlapEtlMetricsCountersManager OlapMetrics = new OlapEtlMetricsCountersManager();

        private Timer _timer;
        private const long MinTimeToWait = 1000;
        private readonly string _tmpFilePath;

        public OlaptEtl(Transformation transformation, OlapEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, OlaptEtlTag)
        {
            Metrics = OlapMetrics;

            var connection = configuration.Connection;

            connection.LocalSettings = BackupTask.GetBackupConfigurationFromScript(connection.LocalSettings, x => JsonDeserializationServer.OlapEtlLocalSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);
            connection.S3Settings = BackupTask.GetBackupConfigurationFromScript(connection.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                    Database, updateServerWideSettingsFunc: null, serverWide: false);

            _tmpFilePath = connection.LocalSettings?.FolderPath ?? 
                              (database.Configuration.Storage.TempPath ?? database.Configuration.Core.DataDirectory).FullPath;

            var etlFrequency = configuration.ETLFrequency;
            var dueTime = GetDueTime(etlFrequency);

            _timer = new Timer(_ => _waitForChanges.Set(), null, dueTime, etlFrequency);
        }

        private TimeSpan GetDueTime(TimeSpan etlFrequency)
        {
            var state = GetProcessState(Database, Configuration.Name, Transformation.Name);
            if (state.LastBatchTime <= 0) 
                return TimeSpan.Zero;
            
            // todo test this

            var nowMs = Database.Time.GetUtcNow().EnsureMilliseconds().Ticks / 10_000;
            var timeSinceLastBatch = nowMs - state.LastBatchTime;

            var dueTime = etlFrequency.TotalMilliseconds - timeSinceLastBatch;

            if (dueTime < MinTimeToWait)
                return TimeSpan.Zero;

            return TimeSpan.FromMilliseconds(dueTime);
        }

        public override EtlType EtlType => EtlType.Olap;

        private static readonly IEnumerator<ToOlapItem> EmptyEnumerator = Enumerable.Empty<ToOlapItem>().GetEnumerator();

        protected override IEnumerator<ToOlapItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToOlapItems(docs, collection);
        }

        protected override IEnumerator<ToOlapItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
        {
            return EmptyEnumerator;
        }

        protected override IEnumerator<ToOlapItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by OLAP ETL");
        }

        protected override IEnumerator<ToOlapItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
        {
            throw new NotSupportedException("Counters aren't supported by OLAP ETL");
        }

        protected override IEnumerator<ToOlapItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
        {
            // todo
            throw new NotImplementedException();
        }

        protected override IEnumerator<ToOlapItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series deletes aren't supported by OLAP ETL");
        }

        protected override bool ShouldUpdateOnLastBatch => true;


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

        protected override EtlTransformer<ToOlapItem, RowGroups> GetTransformer(DocumentsOperationContext context)
        {
            return new OlapDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<RowGroups> records, DocumentsOperationContext context)
        {
            var count = 0;

            foreach (var rowGroups in records)
            {
                var fields = rowGroups.Fields.Values;
                var localPath = GetPath(rowGroups, out var remotePath);

                using (Stream fileStream = File.OpenWrite(localPath))
                {
                    using (var parquetWriter = new ParquetWriter(new Schema(fields), fileStream))
                    {
                        foreach (var group in rowGroups.Groups)
                        {
                            WriteGroup(parquetWriter, group, rowGroups);
                            LogStats(group, rowGroups.TableName, rowGroups.PartitionKey);
                            count += group.Count;
                        }
                    }
                }

                using (Stream fileStream = File.OpenRead(localPath))
                {
                    UploadToDestination(fileStream, remotePath);
                }

                if (Configuration.Connection.LocalSettings?.KeepFilesOnDisc ?? false)
                    continue;
                
                File.Delete(localPath);

            }

            return count;
        }

        private static void WriteGroup(ParquetWriter parquetWriter, RowGroup group, RowGroups rowGroups)
        {
            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
            {
                foreach (var kvp in group.Data)
                {
                    if (rowGroups.Fields.TryGetValue(kvp.Key, out var field) == false)
                        continue;

                    var data = kvp.Value;
                    Array array = default;

                    // todo handle more types
                    switch (field.DataType)
                    {
                        case DataType.Unspecified:
                            // todo
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
                }
            }
        }

        private void UploadToDestination(Stream stream, string path)
        {
            var s3Settings = Configuration.Connection.S3Settings;
            if (s3Settings != null)
            {
                var key = GetKey(s3Settings, path);
                UploadToS3(s3Settings, stream, key);
            }
        }


        private void UploadToS3(S3Settings s3Settings, Stream stream, string key)
        {
            using (var client = new RavenAwsS3Client(s3Settings, progress: null, Logger, CancellationToken))
            {
                client.PutObject(key, stream, new Dictionary<string, string>
                {
                    {"Description", $"Parquet ETL to S3 for db {Database.Name} at {SystemTime.UtcNow}"}
                });
            }
        }

        private static string GetKey(S3Settings s3Settings, string path)
        {
            var prefix = string.IsNullOrWhiteSpace(s3Settings.RemoteFolderName)
                ? string.Empty
                : $"{s3Settings.RemoteFolderName}";

            return $"{prefix}/{path}";
        }

        private void LogStats(RowGroup group, string name, string key)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"[{Name}] Inserted {group.Count} records to '{name}/{key}' table " +
                            $"from the following documents: {string.Join(", ", group.Ids)}");
            }
        }

        private string GetPath(RowGroups group, out string remotePath)
        {
            var fileName = $"{Database.Name}_{Guid.NewGuid()}.parquet";
            remotePath = $"{group.PartitionKey}/{fileName}";

            return Path.Combine(_tmpFilePath, fileName);
        }
    }
}
