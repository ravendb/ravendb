using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Patch;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ResolveConflictOnReplicationConfigurationChange
    {
        private readonly DocumentDatabase _database;
        private readonly Logger _log;
        private readonly ReplicationLoader _replicationLoader;

        public Task ResolveConflictsTask = Task.CompletedTask;

        internal Dictionary<string, ScriptResolver> ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
        public ConflictSolver ConflictSolver => _replicationLoader.ConflictSolverConfig;

        public ResolveConflictOnReplicationConfigurationChange(ReplicationLoader replicationLoader, Logger log)
        {
            _replicationLoader = replicationLoader ??
                throw new ArgumentNullException($"{nameof(ResolveConflictOnReplicationConfigurationChange)} must have replicationLoader instance");
            _database = _replicationLoader.Database;
            _log = log;
        }

        public long ConflictsCount => _database.DocumentsStorage?.ConflictsStorage?.ConflictsCount ?? 0;

        public void RunConflictResolversOnce()
        {
            UpdateScriptResolvers();

            if (ConflictsCount > 0 && ConflictSolver?.IsEmpty() == false)
            {
                try
                {
                    ResolveConflictsTask.Wait(TimeSpan.FromSeconds(60));
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Failed to wait for a previous task of automatic conflict resolution", e);
                }
                ResolveConflictsTask = Task.Run(ResolveConflictsInBackground);
            }
        }

        private async Task ResolveConflictsInBackground()
        {
            try
            {
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (context.OpenReadTransaction())
                    {
                        var resolvedConflicts = new List<(DocumentConflict ResolvedConflict, long MaxConflictEtag)>();

                        var hadConflicts = false;

                        foreach (var conflicts in _database.DocumentsStorage.ConflictsStorage.GetAllConflictsBySameId(context))
                        {
                            if (_database.DatabaseShutdown.IsCancellationRequested)
                                break;

                            hadConflicts = true;

                            var collection = conflicts[0].Collection;

                            var maxConflictEtag = conflicts.Max(x => x.Etag);

                            DocumentConflict resolved;
                            if (ScriptConflictResolversCache.TryGetValue(collection, out var scriptResolver) && scriptResolver != null)
                            {
                                if (TryResolveConflictByScriptInternal(
                                    context,
                                    scriptResolver,
                                    conflicts,
                                    collection,
                                    resolvedConflict: out resolved))
                                {
                                    resolved.Flags = resolved.Flags.Strip(DocumentFlags.FromReplication);
                                    resolvedConflicts.Add((resolved, maxConflictEtag));

                                    //stats.AddResolvedBy(collection + " Script", conflictList.Count);
                                    continue;
                                }
                            }

                            if (ConflictSolver?.ResolveToLatest == true)
                            {
                                resolved = ResolveToLatest(context, conflicts);
                                resolved.Flags = resolved.Flags.Strip(DocumentFlags.FromReplication);
                                resolvedConflicts.Add((resolved, maxConflictEtag));

                                //stats.AddResolvedBy("ResolveToLatest", conflictList.Count);
                            }
                        }

                        if (hadConflicts == false || _database.DatabaseShutdown.IsCancellationRequested)
                            return;

                        if (resolvedConflicts.Count > 0)
                        {
                            var cmd = new PutResolvedConflictsCommand(_database.DocumentsStorage.ConflictsStorage, resolvedConflicts, this);
                            await _database.TxMerger.Enqueue(cmd);
                            if (cmd.RequiresRetry)
                                RunConflictResolversOnce();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Failed to run automatic conflict resolution", e);
            }
        }

        internal class PutResolvedConflictsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly ConflictsStorage _conflictsStorage;
            private readonly List<(DocumentConflict ResolvedConflict, long MaxConflictEtag)> _resolvedConflicts;
            private readonly ResolveConflictOnReplicationConfigurationChange _resolver;
            public bool RequiresRetry;

            public PutResolvedConflictsCommand(ConflictsStorage conflictsStorage, List<(DocumentConflict, long)> resolvedConflicts, ResolveConflictOnReplicationConfigurationChange resolver)
            {
                _conflictsStorage = conflictsStorage;
                _resolvedConflicts = resolvedConflicts;
                _resolver = resolver;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var count = 0;

                foreach (var item in _resolvedConflicts)
                {
                    count++;

                    using (Slice.External(context.Allocator, item.ResolvedConflict.LowerId, out var lowerId))
                    {
                        // let's check if nothing has changed since we resolved the conflict in the read tx
                        // in particular the conflict could be resolved externally before the tx merger opened this write tx

                        if (_conflictsStorage.ShouldThrowConcurrencyExceptionOnConflict(context, lowerId, item.MaxConflictEtag, out _))
                            continue;
                        RequiresRetry = true;
                    }

                    _resolver.PutResolvedDocument(context, item.ResolvedConflict);
                }

                return count;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                // The LowerId created as in memory LazyStringValue that doesn't have escape characters
                // so EscapePositions set to empty to avoid reference to escape bytes (after string bytes) while serializing
                foreach (var conflict in _resolvedConflicts)
                {
                    conflict.ResolvedConflict.LowerId.EscapePositions = Array.Empty<int>();
                }

                return new PutResolvedConflictsCommandDto
                {
                    ResolvedConflicts = _resolvedConflicts,
                };
            }
        }

        private void UpdateScriptResolvers()
        {
            if (ConflictSolver?.ResolveByCollection == null)
            {
                if (ScriptConflictResolversCache.Count > 0)
                    ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
                return;
            }
            var copy = new Dictionary<string, ScriptResolver>();
            foreach (var kvp in ConflictSolver.ResolveByCollection)
            {
                var collection = kvp.Key;
                var script = kvp.Value.Script;
                if (string.IsNullOrEmpty(script.Trim()))
                {
                    continue;
                }
                copy[collection] = new ScriptResolver
                {
                    Script = script
                };
            }
            ScriptConflictResolversCache = copy;
        }

        private bool ValidatedResolveByScriptInput(ScriptResolver scriptResolver,
            IReadOnlyList<DocumentConflict> conflicts,
            LazyStringValue collection)
        {
            if (scriptResolver == null)
                return false;
            if (collection == null)
                return false;
            if (conflicts.Count < 2)
                return false;

            foreach (var documentConflict in conflicts)
            {
                if (collection != documentConflict.Collection)
                {
                    var msg = $"All conflicted documents must have same collection name, but we found conflicted document in {collection} and an other one in {documentConflict.Collection}";
                    if (_log.IsInfoEnabled)
                        _log.Info(msg);

                    var differentCollectionNameAlert = AlertRaised.Create(
                        _database.Name,
                        $"Script unable to resolve conflicted documents with the ID {documentConflict.Id}",
                        msg,
                        AlertType.Replication,
                        NotificationSeverity.Error,
                        "Mismatched Collections On Replication Resolve"
                        );
                    _database.NotificationCenter.Add(differentCollectionNameAlert);
                    return false;
                }
            }

            return true;
        }

        public void PutResolvedDocument(
           DocumentsOperationContext context,
           DocumentConflict resolved,
           DocumentConflict incoming = null)
        {
            SaveConflictedDocumentsAsRevisions(context, resolved, incoming);

            if (resolved.Doc == null)
            {
                using (Slice.External(context.Allocator, resolved.LowerId, out var lowerId))
                {
                    _database.DocumentsStorage.Delete(context, lowerId, resolved.Id, null,
                        _database.Time.GetUtcNow().Ticks, resolved.ChangeVector, new CollectionName(resolved.Collection),
                        documentFlags: resolved.Flags | DocumentFlags.Resolved | DocumentFlags.HasRevisions, nonPersistentFlags: NonPersistentDocumentFlags.FromResolver | NonPersistentDocumentFlags.Resolved);
                    return;
                }
            }

            // because we are resolving to a conflict, and putting a document will
            // delete all the conflicts, we have to create a copy of the document
            // in order to avoid the data we are saving from being removed while
            // we are saving it

            // the resolved document could be an update of the existing document, so it's a good idea to clone it also before updating.
            using (var clone = resolved.Doc.Clone(context))
            {
                // handle the case where we resolve a conflict for a document from a different collection
                DeleteDocumentFromDifferentCollectionIfNeeded(context, resolved);

                ReplicationUtils.EnsureCollectionTag(clone, resolved.Collection);
                // we always want to merge the counters and attachments, even if the user specified a script
                var nonPersistentFlags = NonPersistentDocumentFlags.ResolveCountersConflict | NonPersistentDocumentFlags.ResolveAttachmentsConflict |
                                         NonPersistentDocumentFlags.FromResolver | NonPersistentDocumentFlags.Resolved;
                _database.DocumentsStorage.Put(context, resolved.Id, null, clone, null, resolved.ChangeVector, resolved.Flags | DocumentFlags.Resolved, nonPersistentFlags: nonPersistentFlags);
            }
        }

        private void SaveConflictedDocumentsAsRevisions(DocumentsOperationContext context, DocumentConflict resolved, DocumentConflict incoming)
        {
            if (incoming == null)
                return;

            // we resolved the conflict on the fly, so we save the remote document as revisions
            if (incoming.Doc != null)
            {
                _database.DocumentsStorage.RevisionsStorage.Put(context, incoming.Id, incoming.Doc,
                    incoming.Flags.Strip(DocumentFlags.FromClusterTransaction) | DocumentFlags.Conflicted | DocumentFlags.HasRevisions,
                    NonPersistentDocumentFlags.FromResolver, incoming.ChangeVector, incoming.LastModified.Ticks);
            }
            else
            {
                using (Slice.External(context.Allocator, incoming.LowerId, out var lowerId))
                {
                    _database.DocumentsStorage.RevisionsStorage.Delete(context, incoming.Id, lowerId, new CollectionName(incoming.Collection), incoming.ChangeVector,
                        incoming.LastModified.Ticks, NonPersistentDocumentFlags.FromResolver, incoming.Flags | DocumentFlags.Conflicted | DocumentFlags.HasRevisions);
                }
            }

            if (_database.DocumentsStorage.ConflictsStorage.ConflictsCount != 0) // we have conflicts and we will resolve them in the put method, rest of the function is when we resolve on the fly
                return;

            SaveLocalAsRevision(context, resolved.Id);
        }

        public void SaveLocalAsRevision(DocumentsOperationContext context, string id)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
            if (existing.Document != null)
            {
                _database.DocumentsStorage.RevisionsStorage.Put(context, existing.Document.Id, existing.Document.Data,
                    existing.Document.Flags | DocumentFlags.Conflicted | DocumentFlags.HasRevisions,
                    NonPersistentDocumentFlags.FromResolver, existing.Document.ChangeVector, existing.Document.LastModified.Ticks);
            }
            else if (existing.Tombstone != null)
            {
                using (Slice.External(context.Allocator, existing.Tombstone.LowerId, out var key))
                {
                    _database.DocumentsStorage.RevisionsStorage.Delete(context, existing.Tombstone.LowerId, key, new CollectionName(existing.Tombstone.Collection),
                        existing.Tombstone.ChangeVector,
                        existing.Tombstone.LastModified.Ticks, NonPersistentDocumentFlags.FromResolver,
                        existing.Tombstone.Flags | DocumentFlags.Conflicted | DocumentFlags.HasRevisions);
                }
            }
        }

        private void DeleteDocumentFromDifferentCollectionIfNeeded(DocumentsOperationContext ctx, DocumentConflict conflict)
        {
            // if already conflicted, don't need to do anything
            var oldVersion = _database.DocumentsStorage.Get(ctx, conflict.LowerId, throwOnConflict: false);

            if (oldVersion == null)
                return;

            var oldVersionCollectionName = CollectionName.GetCollectionName(oldVersion.Data);
            if (oldVersionCollectionName.Equals(conflict.Collection, StringComparison.OrdinalIgnoreCase))
                return;

            _database.DocumentsStorage.DeleteWithoutCreatingTombstone(ctx, oldVersionCollectionName, oldVersion.StorageId, isTombstone: false);
        }

        public bool TryResolveConflictByScriptInternal(
            DocumentsOperationContext context,
            ScriptResolver scriptResolver,
            IReadOnlyList<DocumentConflict> conflicts,
            LazyStringValue collection,
            out DocumentConflict resolvedConflict)
        {
            resolvedConflict = null;

            if (ValidatedResolveByScriptInput(scriptResolver, conflicts, collection) == false)
                return false;

            var patch = new PatchConflict(_database, conflicts);
            var updatedConflict = conflicts[0];
            var patchRequest = new PatchRequest(scriptResolver.Script, PatchRequestType.Conflict);
            if (patch.TryResolveConflict(context, patchRequest, out BlittableJsonReaderObject resolved) == false)
            {
                return false;
            }

            updatedConflict.Doc = resolved;
            updatedConflict.Collection = collection;
            updatedConflict.ChangeVector = ChangeVectorUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());

            resolvedConflict = updatedConflict;

            return true;
        }

        public DocumentConflict ResolveToLatest(DocumentsOperationContext context, List<DocumentConflict> conflicts)
        {
            // we have to sort this here because we need to ensure that all the nodes are always 
            // arrive to the same conclusion, regardless of what time they go it
            conflicts.Sort((x, y) => string.Compare(x.ChangeVector, y.ChangeVector, StringComparison.Ordinal));

            var latestDoc = conflicts[0];
            var latestTime = latestDoc.LastModified.Ticks;

            foreach (var documentConflict in conflicts)
            {
                if (documentConflict.LastModified.Ticks > latestTime)
                {
                    latestDoc = documentConflict;
                    latestTime = documentConflict.LastModified.Ticks;
                }
            }

            latestDoc.ChangeVector = ChangeVectorUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());

            return latestDoc;
        }
    }

    internal class PutResolvedConflictsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand>
    {
        public List<(DocumentConflict ResolvedConflict, long MaxConflictEtag)> ResolvedConflicts;

        public ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var resolver = new ResolveConflictOnReplicationConfigurationChange(
                database.ReplicationLoader,
                LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name));

            return new ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand(
                database.DocumentsStorage.ConflictsStorage,
                ResolvedConflicts,
                resolver);
        }
    }
}
