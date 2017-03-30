using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Patch;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
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

        public ResolveConflictOnReplicationConfigurationChange(ReplicationLoader replicationLoader, Logger log)
        {
            _replicationLoader = replicationLoader;
            _database = _replicationLoader.Database;
            _log = log;
        }

        public long ConflictsCount => _database.DocumentsStorage.ConflictsStorage.ConflictsCount;

        public void RunConflictResolversOnce()
        {
            UpdateScriptResolvers();

            if (ConflictsCount > 0)
            {
                try
                {
                    ResolveConflictsTask.Wait();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Failed to wait for a previous task of automatic conflict resolution", e);
                }
                ResolveConflictsTask = Task.Run(() =>
                {
                    try
                    {
                        ResolveConflictsInBackground();
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Failed to run automatic conflict resolution", e);
                    }
                });
            }
        }

        private void ResolveConflictsInBackground()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                Slice lastKey;
                Slice.From(context.Allocator, string.Empty, out lastKey);
                try
                {
                    bool hasConflicts = true;
                    var timeout = 150;
                    if (Debugger.IsAttached)
                        timeout *= 10;
                    while (hasConflicts && !_database.DatabaseShutdown.IsCancellationRequested)
                    {
                        try
                        {
                            var sp = Stopwatch.StartNew();
                            DocumentsTransaction tx = null;
                            try
                            {
                                try
                                {
                                    tx = context.OpenWriteTransaction();
                                }
                                catch (TimeoutException)
                                {
                                    continue;
                                }
                                hasConflicts = false;
                                while (!_database.DatabaseShutdown.IsCancellationRequested)
                                {
                                    if (sp.ElapsedMilliseconds > timeout)
                                    {
                                        // we must release the write transaction to avoid
                                        // completely blocking all other operations.
                                        // This is a background task that we can leave later
                                        hasConflicts = true;
                                        break;
                                    }

                                    var conflicts = _database.DocumentsStorage.ConflictsStorage.GetAllConflictsBySameKeyAfter(context, ref lastKey);
                                    if (conflicts.Count == 0)
                                        break;
                                    if (TryResolveConflict(context, conflicts) == false)
                                        continue;
                                    hasConflicts = true;
                                }

                                tx.Commit();
                            }
                            finally
                            {
                                tx?.Dispose();
                            }
                        }
                        finally
                        {
                            if (lastKey.HasValue)
                                lastKey.Release(context.Allocator);

                            Slice.From(context.Allocator, string.Empty, out lastKey);
                        }
                    }
                }
                finally
                {
                    if (lastKey.HasValue)
                        lastKey.Release(context.Allocator);
                }
            }
        }

        private bool TryResolveConflict(DocumentsOperationContext context, List<DocumentConflict> conflictList)
        {
            var collection = conflictList[0].Collection;

            ScriptResolver scriptResovler;
            if (ScriptConflictResolversCache.TryGetValue(collection, out scriptResovler) &&
                scriptResovler != null)
            {
                if (TryResolveConflictByScriptInternal(
                    context,
                    scriptResovler,
                    conflictList,
                    collection,
                    hasLocalTombstone: false))
                {
                    //stats.AddResolvedBy(collection + " Script", conflictList.Count);
                    return true;
                }

            }

            if (TryResolveUsingDefaultResolverInternal(
                context,
                _replicationLoader.ReplicationDocument?.DefaultResolver,
                conflictList))
            {
                //stats.AddResolvedBy("DatabaseResolver", conflictList.Count);
                return true;
            }

            if (_replicationLoader.ReplicationDocument?.DocumentConflictResolution == StraightforwardConflictResolution.ResolveToLatest)
            {
                ResolveToLatest(context, conflictList);
                //stats.AddResolvedBy("ResolveToLatest", conflictList.Count);
                return true;
            }

            return false;
        }

        private void UpdateScriptResolvers()
        {
            if (_replicationLoader.ReplicationDocument?.ResolveByCollection == null)
            {
                if (ScriptConflictResolversCache.Count > 0)
                    ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
                return;
            }
            var copy = new Dictionary<string, ScriptResolver>();
            foreach (var kvp in _replicationLoader.ReplicationDocument.ResolveByCollection)
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

        public bool TryResolveUsingDefaultResolverInternal(
            DocumentsOperationContext context,
            DatabaseResolver resolver,
            IReadOnlyList<DocumentConflict> conflicts)
        {
            if (resolver?.ResolvingDatabaseId == null)
            {
                return false;
            }

            DocumentConflict resolved = null;
            long maxEtag = -1;
            foreach (var documentConflict in conflicts)
            {
                foreach (var changeVectorEntry in documentConflict.ChangeVector)
                {
                    if (changeVectorEntry.DbId.Equals(new Guid(resolver.ResolvingDatabaseId)))
                    {
                        if (changeVectorEntry.Etag == maxEtag)
                        {
                            // we have two documents with same etag of the leader
                            return false;
                        }

                        if (changeVectorEntry.Etag < maxEtag)
                            continue;

                        maxEtag = changeVectorEntry.Etag;
                        resolved = documentConflict;
                        break;
                    }
                }
            }

            if (resolved == null)
                return false;

            resolved.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, resolved);
            return true;
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
                        $"Script unable to resolve conflicted documents with the key {documentConflict.Key}",
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

        public void PutResolvedDocumentBackToStorage(
           DocumentsOperationContext context,
           DocumentConflict conflict)
        {
            if (conflict.Doc == null)
            {
                Slice loweredKey;
                using (Slice.External(context.Allocator, conflict.LoweredKey, out loweredKey))
                {
                    _database.DocumentsStorage.Delete(context, loweredKey, conflict.Key, null,
                        _database.Time.GetUtcNow().Ticks, conflict.ChangeVector, conflict.Collection);
                    return;
                }
            }

            // because we are resolving to a conflict, and putting a document will
            // delete all the conflicts, we have to create a copy of the document
            // in order to avoid the data we are saving from being removed while
            // we are saving it

            // the resolved document could be an update of the existing document, so it's a good idea to clone it also before updating.
            using (var clone = conflict.Doc.Clone(context))
            {
                // handle the case where we resolve a conflict for a document from a different collection
                DeleteDocumentFromDifferentCollectionIfNeeded(context, conflict);

                ReplicationUtils.EnsureCollectionTag(clone, conflict.Collection);
                _database.DocumentsStorage.Put(context, conflict.LoweredKey, null, clone, null, conflict.ChangeVector);
            }
        }

        private void DeleteDocumentFromDifferentCollectionIfNeeded(DocumentsOperationContext ctx, DocumentConflict conflict)
        {
            Document oldVersion;
            try
            {
                oldVersion = _database.DocumentsStorage.Get(ctx, conflict.LoweredKey);
            }
            catch (DocumentConflictException)
            {
                return; // if already conflicted, don't need to do anything
            }

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
            bool hasLocalTombstone)
        {
            if (ValidatedResolveByScriptInput(scriptResolver, conflicts, collection) == false)
            {
                return false;
            }

            var patch = new PatchConflict(_database, conflicts);
            var updatedConflict = conflicts[0];
            var patchRequest = new PatchRequest
            {
                Script = scriptResolver.Script
            };
            BlittableJsonReaderObject resolved;
            if (patch.TryResolveConflict(context, patchRequest, out resolved) == false)
            {
                return false;
            }

            updatedConflict.Doc = resolved;
            updatedConflict.Collection = collection;
            updatedConflict.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, updatedConflict);
            return true;
        }

        public void ResolveToLatest(
            DocumentsOperationContext context,
            IReadOnlyList<DocumentConflict> conflicts)
        {
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

            latestDoc.ChangeVector = ReplicationUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
            PutResolvedDocumentBackToStorage(context, latestDoc);
        }
    }
}