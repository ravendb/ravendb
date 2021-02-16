using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
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
    public class OlaptEtl : EtlProcess<ToOlapItem, OlapTransformedItems, OlapEtlConfiguration, OlapConnectionString>
    {
        public const string OlaptEtlTag = "OLAP ETL";

        public readonly OlapEtlMetricsCountersManager OlapMetrics = new OlapEtlMetricsCountersManager();

        private Timer _timer;
        private const long MinTimeToWait = 1000;
        private readonly string _tmpFilePath;
        private readonly S3Settings _s3Settings;

        public OlaptEtl(Transformation transformation, OlapEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, OlaptEtlTag)
        {
            Metrics = OlapMetrics;

            var connection = configuration.Connection;

            var localSettings = BackupTask.GetBackupConfigurationFromScript(connection.LocalSettings, x => JsonDeserializationServer.LocalSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            _s3Settings = BackupTask.GetBackupConfigurationFromScript(connection.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                    Database, updateServerWideSettingsFunc: null, serverWide: false);

            _tmpFilePath = localSettings?.FolderPath ?? 
                           (database.Configuration.Storage.TempPath ?? database.Configuration.Core.DataDirectory).FullPath;

            var dueTime = GetDueTime(configuration.RunFrequency);

            _timer = new Timer(_ => _waitForChanges.Set(), null, dueTime, configuration.RunFrequency);
        }

        private TimeSpan GetDueTime(TimeSpan etlFrequency)
        {
            var state = GetProcessState(Database, Configuration.Name, Transformation.Name);
            if (state.LastBatchTime <= 0) 
                return TimeSpan.Zero;
            
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

        protected override void AfterAllBatchesCompleted(long batchTime)
        {
            var batchTimeMs = batchTime / 10_000;
            UpdateEtlProcessState(LastProcessState, batchTimeMs);
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

        protected override EtlTransformer<ToOlapItem, OlapTransformedItems> GetTransformer(DocumentsOperationContext context)
        {
            return new OlapDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<OlapTransformedItems> records, DocumentsOperationContext context)
        {
            var count = 0;

            foreach (var transformed in records)
            {
                var localPath = GetPath(transformed, out var remotePath);
                transformed.GenerateFileFromItems(localPath, Logger);

                using (Stream fileStream = File.OpenRead(localPath))
                {
                    UploadToDestination(fileStream, remotePath);
                }

                if (Configuration.KeepFilesOnDisk)
                    continue;
                
                File.Delete(localPath);
            }

            return count;
        }

        private void UploadToDestination(Stream stream, string path)
        {
            if (_s3Settings != null)
            {
                var key = GetKey(_s3Settings, path);
                UploadToS3(_s3Settings, stream, key);
            }
        }

        private void UploadToS3(S3Settings s3Settings, Stream stream, string key)
        {
            using (var client = new RavenAwsS3Client(s3Settings, progress: null, Logger, CancellationToken))
            {
                client.PutObject(key, stream, new Dictionary<string, string>
                {
                    {"Description", $"OLAP ETL {Name} to S3 for db {Database.Name} at {SystemTime.UtcNow}"}
                });
            }
        }

        private string GetKey(S3Settings s3Settings, string path)
        {
            var prefix = string.IsNullOrWhiteSpace(s3Settings.RemoteFolderName)
                ? string.Empty
                : $"{s3Settings.RemoteFolderName}";

            if (string.IsNullOrWhiteSpace(Configuration.CustomPrefix) == false)
                prefix = $"{prefix}/{Configuration.CustomPrefix}";

            return $"{prefix}/{path}";
        }


        private string GetPath(OlapTransformedItems transformed, out string remotePath)
        {
            var fileName = $"{Database.Name}__{Guid.NewGuid()}.{transformed.Format}";
            remotePath = $"{transformed.Prefix}/{fileName}";

            return Path.Combine(_tmpFilePath, fileName);
        }
    }
}
