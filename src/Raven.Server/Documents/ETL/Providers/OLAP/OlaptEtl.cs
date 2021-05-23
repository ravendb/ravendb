using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using NCrontab.Advanced;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlapEtl : EtlProcess<ToOlapItem, OlapTransformedItems, OlapEtlConfiguration, OlapConnectionString, OlapEtlStatsScope, OlapEtlPerformanceOperation>
    {
        public const string OlaptEtlTag = "OLAP ETL";

        public readonly OlapEtlMetricsCountersManager OlapMetrics = new OlapEtlMetricsCountersManager();

        private PeriodicBackup.PeriodicBackup.BackupTimer _timer;
        private readonly OperationCancelToken _operationCancelToken;
        private static readonly IEnumerator<ToOlapItem> EmptyEnumerator = Enumerable.Empty<ToOlapItem>().GetEnumerator();

        private OlapEtlStatsScope _uploadScope;
        private UploaderSettings _uploaderSettings;

        public OlapEtl(Transformation transformation, OlapEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, OlaptEtlTag)
        {
            Metrics = OlapMetrics;

            GenerateUploaderSetting();

            _operationCancelToken = new OperationCancelToken(Database.DatabaseShutdown, CancellationToken);

            UpdateTimer(LastProcessState.LastBatchTime);
        }

        private void GenerateUploaderSetting()
        {
            var s3Settings = BackupTask.GetBackupConfigurationFromScript(Configuration.Connection.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            var azureSettings = BackupTask.GetBackupConfigurationFromScript(Configuration.Connection.AzureSettings, x => JsonDeserializationServer.AzureSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            var glacierSettings = BackupTask.GetBackupConfigurationFromScript(Configuration.Connection.GlacierSettings, x => JsonDeserializationServer.GlacierSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            var googleCloudSettings = BackupTask.GetBackupConfigurationFromScript(Configuration.Connection.GoogleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            _uploaderSettings = new UploaderSettings
            {
                S3Settings = s3Settings, 
                AzureSettings = azureSettings, 
                GlacierSettings = glacierSettings,
                GoogleCloudSettings = googleCloudSettings,
                DatabaseName = Database.Name, 
                TaskName = Name
            };
        }

        public override EtlType EtlType => EtlType.Olap;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        public override void NotifyAboutWork(DatabaseChange change)
        {
            // intentionally not setting _waitForChanges here
            // _waitForChanges is being set by the timer
        }

        protected override OlapEtlStatsScope CreateScope(EtlRunStats stats)
        {
            return new OlapEtlStatsScope(stats);
        }

        protected override bool ShouldTrackAttachmentTombstones()
        {
            return false;
        }

        protected override bool ShouldFilterOutHiLoDocument()
        {
            return true;
        }

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

        protected override EtlTransformer<ToOlapItem, OlapTransformedItems, OlapEtlStatsScope, OlapEtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
        {
            return new OlapDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<OlapTransformedItems> records, DocumentsOperationContext context, OlapEtlStatsScope scope)
        {
            var count = 0;

            var outerScope = scope.For(EtlOperations.LoadLocal, start: false);

            foreach (var transformed in records)
            {
                outerScope.NumberOfFiles++;

                string localPath, folderName, fileName, safeFolderName;
                using (outerScope.Start())
                using (var loadScope = outerScope.For($"{EtlOperations.LoadLocal}/{outerScope.NumberOfFiles}"))
                {
                    localPath = transformed.GenerateFile(out folderName, out safeFolderName, out fileName);

                    loadScope.FileName = fileName;
                    loadScope.NumberOfFiles = 1;

                    count += transformed.Count;
                }

                if (AnyRemoteDestinations())
                    UploadToServer(localPath, folderName, fileName, safeFolderName, scope);

                if (Configuration.Connection.LocalSettings != null)
                    continue;

                File.Delete(localPath);
            }

            return count;
        }

        protected override void AfterAllBatchesCompleted(DateTime lastBatchTime)
        {
            if (Statistics.LastProcessedEtag > 0)
                UpdateEtlProcessState(LastProcessState, lastBatchTime);

            UpdateTimer(lastBatchTime);
        }

        private void UpdateTimer(DateTime? lastBatchTime)
        {
            var nextRunOccurrence = GetNextRunOccurrence(Configuration.RunFrequency, lastBatchTime);

            var now = SystemTime.UtcNow;
            var timeSpan = nextRunOccurrence - now;

            TimeSpan nextRunTimeSpan;
            if (timeSpan.Ticks <= 0)
            {
                nextRunTimeSpan = TimeSpan.Zero;
                nextRunOccurrence = now;
            }
            else
            {
                if (_timer?.NextBackup?.DateTime == nextRunOccurrence)
                    return;

                nextRunTimeSpan = timeSpan;
            }

            UpdateTimerInternal(new NextBackup
            {
                TimeSpan = nextRunTimeSpan,
                DateTime = nextRunOccurrence
            });
        }

        private void UpdateTimerInternal(NextBackup nextRun)
        {
            if (Configuration.Disabled)
                return;

            _timer?.Dispose();

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"OLAP ETL '{Name}' : Next run is in {nextRun.TimeSpan.TotalMinutes} minutes.");

            var timer = new Timer(_ => _waitForChanges.Set(), state: nextRun, dueTime: nextRun.TimeSpan, period: Timeout.InfiniteTimeSpan);

            _timer = new PeriodicBackup.PeriodicBackup.BackupTimer
            {
                Timer = timer,
                CreatedAt = DateTime.UtcNow,
                NextBackup = nextRun
            };
        }

        private DateTime GetNextRunOccurrence(string runFrequency, DateTime? lastBatchTime = null)
        {
            if (string.IsNullOrWhiteSpace(runFrequency))
                return default;

            try
            {
                var backupParser = CrontabSchedule.Parse(runFrequency);
                return backupParser.GetNextOccurrence(lastBatchTime ?? default);
            }
            catch (Exception e)
            {
                var message = "Couldn't parse OLAP ETL " +
                              $"frequency {runFrequency}, task id: {Configuration.TaskId}, " +
                              $"ETL name: {Name} , error: {e.Message}";


                if (Logger.IsOperationsEnabled)
                    Logger.Operations(message);

                Database.NotificationCenter.Add(AlertRaised.Create(
                    Database.Name,
                    "OLAP ETL run frequency parsing error",
                    message,
                    AlertType.Etl_Error,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                return default;
            }
        }

        private static BackupResult GenerateUploadResult()
        {
            return new BackupResult
            {
                // Skipped will be set later, if needed
                S3Backup = new UploadToS3
                {
                    Skipped = true
                },
                AzureBackup = new UploadToAzure
                {
                    Skipped = true
                },
                GoogleCloudBackup = new UploadToGoogleCloud
                {
                    Skipped = true
                },
                GlacierBackup = new UploadToGlacier
                {
                    Skipped = true
                },
                FtpBackup = new UploadToFtp
                {
                    Skipped = true
                }
            };
        }


        private void UploadToServer(string localPath, string folderName, string fileName, string safeFolderName, OlapEtlStatsScope scope)
        {
            CancellationToken.ThrowIfCancellationRequested();
            
            _uploaderSettings.FilePath = localPath;
            _uploaderSettings.FileName = fileName;
            _uploaderSettings.FolderName = folderName;
            _uploaderSettings.SafeFolderName = safeFolderName;
            
            var backupUploader = new BackupUploader(uploaderSettings, new RetentionPolicyBaseParameters(), Logger, GenerateUploadResult(), onProgress: ProgressNotification, _operationCancelToken);

            try
            {
                OlapEtlStatsScope outerScope = null;
                if (backupUploader.AnyUploads)
                {
                    outerScope = scope.For(EtlOperations.LoadUpload, start: true);
                    outerScope.NumberOfFiles++;

                    _uploadScope = outerScope.For($"{EtlOperations.LoadUpload}/{outerScope.NumberOfFiles}", start: true);
                    _uploadScope.FileName = fileName;
                    _uploadScope.NumberOfFiles = 1;
                }

                using (outerScope)
                using (_uploadScope)
                    backupUploader.Execute();
            }
            finally
            {
                _uploadScope = null;
            }
        }

        private void ProgressNotification(IOperationProgress progress)
        {
            var uploadScope = _uploadScope;
            if (uploadScope == null)
                return;

            var backupProgress = progress as BackupProgress;
            if (backupProgress == null)
                return;

            uploadScope.AzureUpload = GetUploadProgress(backupProgress.AzureBackup);
            uploadScope.FtpUpload = GetUploadProgress(backupProgress.FtpBackup);
            uploadScope.GlacierUpload = GetUploadProgress(backupProgress.GlacierBackup);
            uploadScope.GoogleCloudUpload = GetUploadProgress(backupProgress.GoogleCloudBackup);
            uploadScope.S3Upload = GetUploadProgress(backupProgress.S3Backup);

            static UploadProgress GetUploadProgress(CloudUploadStatus current)
            {
                if (current == null || current.Skipped)
                    return null;

                return current.UploadProgress;
            }
        }

        private bool AnyRemoteDestinations() =>
            _uploaderSettings.S3Settings != null || 
            _uploaderSettings.GlacierSettings != null || 
            _uploaderSettings.AzureSettings != null || 
            _uploaderSettings.GoogleCloudSettings != null ||
            _uploaderSettings.FtpSettings != null;
    }
}
