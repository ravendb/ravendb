using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlapEtl : EtlProcess<ToOlapItem, OlapTransformedItems, OlapEtlConfiguration, OlapConnectionString>
    {
        public const string OlaptEtlTag = "OLAP ETL";

        public readonly OlapEtlMetricsCountersManager OlapMetrics = new OlapEtlMetricsCountersManager();

        private Timer _timer;
        private const long MinTimeToWait = 1000;
        private readonly S3Settings _s3Settings;
        private readonly OperationCancelToken _operationCancelToken;

        public OlapEtl(Transformation transformation, OlapEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, OlaptEtlTag)
        {
            Metrics = OlapMetrics;
            Name = $"{Configuration.Name}_{Transformation.Name}";

            _s3Settings = BackupTask.GetBackupConfigurationFromScript(configuration.Connection.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                    Database, updateServerWideSettingsFunc: null, serverWide: false);

            _operationCancelToken = new OperationCancelToken(Database.DatabaseShutdown, CancellationToken);

            _timer = new Timer(_ => _waitForChanges.Set(), state: null, dueTime: GetDueTime(configuration.RunFrequency), period: configuration.RunFrequency);
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
            // RavenDB-16308
            throw new NotSupportedException("Time Series are currently not supported by OLAP ETL");
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
            return new OlapDocumentTransformer(Transformation, Database, context, Configuration, Name, Logger);
        }

        protected override int LoadInternal(IEnumerable<OlapTransformedItems> records, DocumentsOperationContext context)
        {
            var count = 0;

            foreach (var transformed in records)
            {
                var localPath = transformed.GenerateFileFromItems(out var folderName, out var fileName);
                count += transformed.Count;

                UploadToServer(localPath, folderName, fileName);

                if (Configuration.KeepFilesOnDisk)
                    continue;
                
                File.Delete(localPath);
            }

            return count;
        }

        private void UploadToServer(string localPath, string folderName, string fileName)
        {
            CancellationToken.ThrowIfCancellationRequested();

            var uploaderSettings = new UploaderSettings
            {
                S3Settings = _s3Settings,
                FilePath = localPath,
                FolderName = folderName,
                FileName = fileName,
                DatabaseName = Database.Name,
                TaskName = Name
            };

            var uploadResult = new BackupResult
            {
                S3Backup = new UploadToS3
                {
                    Skipped = true // will be set later, if needed
                }
            };

            var backupUploader = new BackupUploader(uploaderSettings, new RetentionPolicyBaseParameters(), Logger, uploadResult, onProgress: ProgressNotification, _operationCancelToken);
            backupUploader.Execute();
        }

        private static void ProgressNotification(IOperationProgress progress)
        {
            // todo RavenDB-16341
        }

    }
}
