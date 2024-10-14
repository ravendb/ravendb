using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Revisions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class RevisionsBinCleaner : BackgroundWorkBase
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly RevisionsBinConfiguration _configuration;
        private readonly long _batchSize;

        public RevisionsBinCleaner(DocumentDatabase documentDatabase, RevisionsBinConfiguration configuration) : base(documentDatabase.Name, documentDatabase.DatabaseShutdown)
        {
            _documentDatabase = documentDatabase;
            _configuration = configuration;
            _batchSize = _documentDatabase.Is32Bits ? 1024 : 10 * 1024;
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
                if (config == null || config.Disabled || ShouldHandleWorkOnCurrentNode(record.Topology, nodeTag) == false)
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

        private static bool ShouldHandleWorkOnCurrentNode(DatabaseTopology topology, string nodeTag) => string.Equals(topology.Members.FirstOrDefault(), nodeTag, StringComparison.OrdinalIgnoreCase);


        internal async Task<long> ExecuteCleanup(RevisionsBinConfiguration config = null)
        {
            var numberOfDeletedEntries = 0L;

            config ??= _configuration;

            if (config == null || 
                config.MinimumEntriesAgeToKeep == null || 
                config.MinimumEntriesAgeToKeep.Value == TimeSpan.MaxValue || 
                CancellationToken.IsCancellationRequested)
                return numberOfDeletedEntries;

            try
            {
                var before = _documentDatabase.Time.GetUtcNow() - config.MinimumEntriesAgeToKeep.Value;

                while (CancellationToken.IsCancellationRequested == false)
                {
                    long newLastEtag;
                    List<string> ids;
                    using(_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var oldLastEtag = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(ctx.Transaction.InnerTransaction);
                        newLastEtag = oldLastEtag;
                        ids = _documentDatabase.DocumentsStorage.RevisionsStorage
                            .GetRevisionsBinEntriesIds(ctx, before, DocumentFields.Id | DocumentFields.ChangeVector, _batchSize, ref newLastEtag);

                        if (newLastEtag <= oldLastEtag)
                            break;
                    }

                    var command = new RevisionsStorage.RevisionsBinCleanMergedCommand(ids, newLastEtag);
                    await _documentDatabase.TxMerger.Enqueue(command);

                    var res = command.Result;
                    numberOfDeletedEntries += res.DeletedEntries;

                    if (ids.Count < _batchSize && res.CanContinueTransaction)
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

    }
}
