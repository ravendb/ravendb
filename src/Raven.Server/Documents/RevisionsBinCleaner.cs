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
        private readonly RevisionsBinConfiguration _configuration;

        public RevisionsBinCleaner(DocumentDatabase documentDatabase, RevisionsBinConfiguration configuration) : base(documentDatabase.Name, documentDatabase.DatabaseShutdown)
        {
            _documentDatabase = documentDatabase;
            _configuration = configuration;
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_configuration.RefreshFrequency);

            await ExecuteCleanup(_configuration);
        }

        public static RevisionsBinCleaner LoadConfigurations(DocumentDatabase database, DatabaseRecord record, RevisionsBinCleaner oldCleaner, string nodeTag)
        {
            try
            {
                var config = record.RevisionsBin;
                if (config == null || config.Disabled || NodeIsResponsible(record.Topology, nodeTag) == false)
                {
                    oldCleaner?.Dispose();
                    return null;
                }

                if (oldCleaner != null && config.Equals(oldCleaner._configuration))
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

        private static bool NodeIsResponsible(DatabaseTopology topology, string nodeTag) => topology.Members[0] == nodeTag;
        

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
                var maxReadsPerBatch = config.MaxItemsToProcess;

                while (CancellationToken.IsCancellationRequested == false)
                {
                    var command = new RevisionsStorage.RevisionsBinCleanMergedCommand(before, maxReadsPerBatch);
                    await _documentDatabase.TxMerger.Enqueue(command);

                    var res = command.Result;

                    numberOfDeletedEntries += res.DeletedEntries;
                    
                     if (res.HasMore == false)
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
