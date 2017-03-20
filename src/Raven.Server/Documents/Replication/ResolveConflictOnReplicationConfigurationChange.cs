using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ResolveConflictOnReplicationConfigurationChange
    {
        private readonly DocumentDatabase _database;
        private readonly Logger _log;
        private readonly DocumentReplicationLoader _documentReplicationLoader;

        public Task ResolveConflictsTask = Task.CompletedTask;

        public ResolveConflictOnReplicationConfigurationChange(DocumentReplicationLoader documentReplicationLoader, Logger log)
        {
            _documentReplicationLoader = documentReplicationLoader;
            _database = _documentReplicationLoader.Database;
            _log = log;
        }

        public long ConflictsCount => _database.DocumentsStorage.ConflictsCount;

        public void RunConflictResolversOnce()
        {
            UpdateScriptResolvers();

            if (_database.DocumentsStorage.ConflictsCount > 0)
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
            var resolverStats = new ReplicationStatistics.ResolverIterationStats();
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                Slice lastKey;
                Slice.From(context.Allocator, string.Empty, out lastKey);
                try
                {
                    bool hasConflicts = true;
                    resolverStats.StartTime = DateTime.UtcNow;
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

                                    var conflicts = _database.DocumentsStorage.GetAllConflictsBySameKeyAfter(context, ref lastKey);
                                    if (conflicts.Count == 0)
                                        break;
                                    if (TryResolveConflict(context, conflicts, resolverStats) == false)
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
                    resolverStats.EndTime = DateTime.UtcNow;
                    resolverStats.ConflictsLeft = ConflictsCount;
                    resolverStats.DefaultResolver = _documentReplicationLoader.ReplicationDocument?.DefaultResolver;
                    _documentReplicationLoader.RepliactionStats.Add(resolverStats);
                }
                finally
                {
                    if (lastKey.HasValue)
                        lastKey.Release(context.Allocator);
                }
            }
        }

        internal Dictionary<string, ScriptResolver> ScriptConflictResolversCache =
            new Dictionary<string, ScriptResolver>();

        private bool TryResolveConflict(DocumentsOperationContext context, List<DocumentConflict> conflictList, ReplicationStatistics.ResolverIterationStats stats)
        {
            var collection = conflictList[0].Collection;

            ScriptResolver scriptResovler;
            if (ScriptConflictResolversCache.TryGetValue(collection, out scriptResovler) &&
                scriptResovler != null)
            {
                if (_database.DocumentsStorage.TryResolveConflictByScriptInternal(
                    context,
                    scriptResovler,
                    conflictList,
                    collection,
                    hasLocalTombstone: false))
                {
                    stats.AddResolvedBy(collection + " Script", conflictList.Count);
                    return true;
                }

            }

            if (_database.DocumentsStorage.TryResolveUsingDefaultResolverInternal(
                context,
                _documentReplicationLoader.ReplicationDocument?.DefaultResolver,
                conflictList,
                hasTombstoneInStorage: false))
            {
                stats.AddResolvedBy("DatabaseResolver", conflictList.Count);
                return true;
            }

            if (_documentReplicationLoader.ReplicationDocument?.DocumentConflictResolution == StraightforwardConflictResolution.ResolveToLatest)
            {
                _database.DocumentsStorage.ResolveToLatest(context, conflictList, false);
                stats.AddResolvedBy("ResolveToLatest", conflictList.Count);
                return true;
            }

            return false;
        }


        private void UpdateScriptResolvers()
        {
            if (_documentReplicationLoader.ReplicationDocument?.ResolveByCollection == null)
            {
                if (ScriptConflictResolversCache.Count > 0)
                    ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
                return;
            }
            var copy = new Dictionary<string, ScriptResolver>();
            foreach (var kvp in _documentReplicationLoader.ReplicationDocument.ResolveByCollection)
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
    }
}