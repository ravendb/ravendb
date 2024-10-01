using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Revisions;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class RevisionsBinCleaner : BackgroundWorkBase
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly int _numberOfRevisionsToDeleteInBatch;
        private readonly RevisionsBinConfiguration _configuration;

        public RevisionsBinCleaner(DocumentDatabase documentDatabase, RevisionsBinConfiguration configuration) : base(documentDatabase.Name, documentDatabase.DatabaseShutdown)
        {
            _documentDatabase = documentDatabase;
            _numberOfRevisionsToDeleteInBatch = _documentDatabase.Is32Bits
                ? 1024
                : 10 * 1024;

            _configuration = configuration;
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_configuration.RefreshFrequency);

            await ExecuteCleanup(_configuration);
        }

        public static RevisionsBinCleaner LoadConfigurations(DocumentDatabase database, DatabaseRecord record, RevisionsBinCleaner oldCleaner)
        {
            try
            {
                var config = record.RevisionsBin;
                if (config == null || config.Disabled)
                {
                    oldCleaner?.Dispose();
                    return null;
                }

                if(oldCleaner != null && config.Equals(oldCleaner._configuration))
                    return oldCleaner;

                oldCleaner?.Dispose();

                var cleaner = new RevisionsBinCleaner(database, config);
                cleaner.Start();
                return cleaner;
            }
            catch (Exception e)
            {
                const string msg = "Cannot enable revisions-bin cleaner as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"Revisions-bin cleaner error in {database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<RevisionsBinCleaner>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                return null;
            }
        }

        internal async Task<long> ExecuteCleanup(RevisionsBinConfiguration config = null)
        {
            var numberOfDeletedEntries = 0L;

            config ??= _configuration;

            if (config == null || 
                config.MinimumEntriesAgeToKeep == null || 
                config.MinimumEntriesAgeToKeep.Value == TimeSpan.MaxValue ||
                config.MaxItemsToProcess <= 0 || 
                CancellationToken.IsCancellationRequested)
                return numberOfDeletedEntries;

            try
            {
                var before = _documentDatabase.Time.GetUtcNow() - config.MinimumEntriesAgeToKeep.Value;
                var batchSize = config.NumberOfDeletesInBatch ?? _numberOfRevisionsToDeleteInBatch;
                var maxReadsPerBatch = config.MaxItemsToProcess;

                var sw = Stopwatch.StartNew();

                while (CancellationToken.IsCancellationRequested == false)
                {
                    var command = new RevisionsStorage.RevisionsBinCleanMergedCommand(before, batchSize, maxReadsPerBatch);
                    await _documentDatabase.TxMerger.Enqueue(command);

                    if (command.Result.HasValue == false)
                        throw new NullReferenceException("RevisionsBinCleanMergedCommand result is null after execution by TxMerger");

                    var res = command.Result.Value;

                    numberOfDeletedEntries += res.DeletedEntries;
                    
                     if (res.HasMore == false || sw.Elapsed > config.CleanupInterval)
                        break;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to execute revisions bin cleanup on {_documentDatabase.Name}", e);
            }

            return numberOfDeletedEntries;
        }

        public class RevisionsBinCleanerState
        {
            public long LastEtag;
        }
    }
}
