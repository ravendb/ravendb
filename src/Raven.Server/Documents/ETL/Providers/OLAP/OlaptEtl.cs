using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class OlapEtl : EtlProcess<ToOlapItem, OlapTransformedItems, OlapEtlConfiguration, OlapConnectionString>
    {
        public const string OlaptEtlTag = "OLAP ETL";

        public readonly OlapEtlMetricsCountersManager OlapMetrics = new OlapEtlMetricsCountersManager();

        private PeriodicBackup.PeriodicBackup.BackupTimer _timer;
        private readonly S3Settings _s3Settings;
        private readonly AzureSettings _azureSettings;
        private readonly BackupResult _uploadResult;
        private readonly OperationCancelToken _operationCancelToken;
        private static readonly IEnumerator<ToOlapItem> EmptyEnumerator = Enumerable.Empty<ToOlapItem>().GetEnumerator();
        private static readonly HashSet<char> SpecialChars = new HashSet<char> { '&', '@', ':', ',', '$', '=', '+', '?', ';', ' ', '"', '^', '`', '>', '<', '{', '}', '[', ']', '#', '\'', '~', '|' };
        private StringBuilder _builder;
        private const string Format = "%{0:X2}";
        private readonly string _fileNamePrefix;

        public OlapEtl(Transformation transformation, OlapEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, OlaptEtlTag)
        {
            Metrics = OlapMetrics;
            Name = $"{Configuration.Name}_{Transformation.Name}";

            _fileNamePrefix = EnsureSafeName(Name, isFolderPath: false);

            _s3Settings = BackupTask.GetBackupConfigurationFromScript(configuration.Connection.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                    Database, updateServerWideSettingsFunc: null, serverWide: false);

            _azureSettings = BackupTask.GetBackupConfigurationFromScript(configuration.Connection.AzureSettings, x => JsonDeserializationServer.AzureSettings(x),
                Database, updateServerWideSettingsFunc: null, serverWide: false);

            _uploadResult = GenerateUploadResult();

            _operationCancelToken = new OperationCancelToken(Database.DatabaseShutdown, CancellationToken);

            UpdateTimer(LastProcessState.LastBatchTime);
        }

        public override EtlType EtlType => EtlType.Olap;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

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

        protected override EtlTransformer<ToOlapItem, OlapTransformedItems> GetTransformer(DocumentsOperationContext context)
        {
            return new OlapDocumentTransformer(Transformation, Database, context, Configuration, _fileNamePrefix, Logger);
        }

        protected override int LoadInternal(IEnumerable<OlapTransformedItems> records, DocumentsOperationContext context)
        {
            var count = 0;
            foreach (var transformed in records)
            {
                var localPath = transformed.GenerateFileFromItems(out var folderName, out var fileName);
                count += transformed.Count;

                UploadToServer(localPath, folderName, fileName);

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
                }
            };
        }

        private void UploadToServer(string localPath, string folderName, string fileName)
        {
            CancellationToken.ThrowIfCancellationRequested();

            var uploaderSettings = new UploaderSettings
            {
                S3Settings = _s3Settings,
                AzureSettings = _azureSettings,
                FilePath = localPath,
                FolderName = EnsureSafeName(folderName),
                FileName = fileName,
                DatabaseName = Database.Name,
                TaskName = Name
            };

            var backupUploader = new BackupUploader(uploaderSettings, new RetentionPolicyBaseParameters(), Logger, _uploadResult, onProgress: ProgressNotification, _operationCancelToken);
            backupUploader.Execute();
        }


        private string EnsureSafeName(string str, bool isFolderPath = true)
        {
            _builder ??= new StringBuilder();
            foreach (char @char in str)
            {
                if (SpecialChars.Contains(@char) || @char <= 31 || @char == 127)
                {
                    _builder.AppendFormat(Format, (int)@char);
                    continue;
                }

                if (isFolderPath == false && @char == '/')
                {
                    _builder.Append('_');
                    continue;
                }

                _builder.Append(@char);
            }

            var val = _builder.ToString();
            _builder.Clear();

            return val;
        }

        private static void ProgressNotification(IOperationProgress progress)
        {
            // RavenDB-16341
        }

    }
}
