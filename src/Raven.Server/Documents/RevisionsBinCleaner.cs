using System;
using System.Collections.Generic;
using System.Text;
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
            await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(_configuration.RefreshFrequencyInSec));

            await ExecuteCleanup(_configuration);
        }

        public static RevisionsBinCleaner LoadConfigurations(DocumentDatabase database, DatabaseRecord record, RevisionsBinCleaner oldCleaner, string nodeTag)
        {
            try
            {
                var config = record.RevisionsBin;

                if (config == null || config.Disabled)
                {
                    oldCleaner?.Dispose();
                    return null;
                }

                if (oldCleaner != null && config.Equals(oldCleaner._configuration))
                    return oldCleaner;

                oldCleaner?.Dispose();

                var cleaner = new RevisionsBinCleaner(database, config);
                cleaner.Start();

                if (cleaner.Logger.IsInfoEnabled)
                    cleaner.Logger.Info($"Executed revisions-bin cleanup on {database.Name} started");

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

        internal bool IsFirst => AbstractBackgroundWorkStorage.ShouldHandleWorkOnCurrentNode(_documentDatabase.ReadDatabaseRecord().Topology, _documentDatabase.ServerStore.NodeTag);

        internal async Task<long> ExecuteCleanup(RevisionsBinConfiguration config = null)
        {
            var totalDeletedEntries = 0L;

            config ??= _configuration;

            if (config == null ||
                config.MinimumEntriesAgeToKeepInMin == null ||
                config.MinimumEntriesAgeToKeepInMin.Value == int.MaxValue ||
                CancellationToken.IsCancellationRequested)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Executed revisions-bin cleanup on {_documentDatabase.Name}, 0 revisions were deleted, finished on etag 0");
                return totalDeletedEntries;
            }

            try
            {
                var before = _documentDatabase.Time.GetUtcNow() - TimeSpan.FromMinutes(config.MinimumEntriesAgeToKeepInMin.Value < 0 ? 0 : config.MinimumEntriesAgeToKeepInMin.Value);

                while (CancellationToken.IsCancellationRequested == false)
                {
                    List<(string Id, long Etag)> idsAndEtags;
                    long readLastEtag;
                    
                    using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        readLastEtag = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(ctx.Transaction.InnerTransaction);
                        idsAndEtags = _documentDatabase.DocumentsStorage.RevisionsStorage
                            .GetDeletedRevisionsIds(ctx, before, _batchSize, ref readLastEtag, Cts.Token);
                    }

                    int idsAndEtagsOriginalCount = idsAndEtags.Count;

                    var isFirst = IsFirst;

                    while (CancellationToken.IsCancellationRequested == false)
                    {
                        var command = new RevisionsStorage.RevisionsBinCleanMergedCommand(idsAndEtags, readLastEtag, isFirst);
                        await _documentDatabase.TxMerger.Enqueue(command);

                        var res = command.Result;
                        totalDeletedEntries += res.DeletedEntries;

                        if (Logger.IsInfoEnabled)
                            Logger.Info(GetLogMsg(res.DeletedEntries, idsAndEtags, res.NextStartIndex, isFirst));
                        

                        if (res.NextStartIndex >= idsAndEtags.Count)
                            break;

                        // we can't actually remove revisions and we skipped whatever we could
                        if (isFirst == false)
                            return 0;

                        idsAndEtags = idsAndEtags.Slice(res.NextStartIndex, idsAndEtags.Count - res.NextStartIndex);
                    }

                    if (idsAndEtagsOriginalCount < _batchSize)
                        return totalDeletedEntries;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to execute revisions bin cleanup on {_documentDatabase.Name}", e);
            }

            return totalDeletedEntries;
        }

        private string GetLogMsg(int deletedEntries, List<(string Id, long Etag)> idsAndEtags, int startIndex, bool isFirst)
        {
            var sb = new StringBuilder();
            sb.Append("Revisions-Bin Cleanup was executed: ");
            if (isFirst)
            {
                sb.Append("Node is first of database \"");
                sb.Append(_documentDatabase.Name);
                sb.Append("\" topology, ");
                sb.Append(deletedEntries);
                sb.Append(" entries were deleted");
            }
            else
            {
                sb.Append(" database \"");
                sb.Append(_documentDatabase.Name);
                if (deletedEntries > 0)
                {
                    sb.Append(", ");
                    sb.Append(deletedEntries);
                    sb.Append(" entries were deleted");
                }
            }

            if (idsAndEtags.Count != 0)
            {
                sb.Append(", finished on etag ");
                sb.Append(idsAndEtags[startIndex == 0 ? 0 : startIndex - 1]);
            }

            return sb.ToString();
        }
    }
}
