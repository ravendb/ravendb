//-----------------------------------------------------------------------
// <copyright file="ReplicationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Client.FileSystem.Extensions;
using Raven.Database;
using Raven.Database.Bundles.Replication;
using Raven.Database.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication.Tasks.Handlers;
using Raven.Database.Config.Retriever;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Bundles.Replication.Tasks
{
    [ExportMetadata("Bundle", "Replication")]
    [InheritedExport(typeof(IStartupTask))]
    public class ReplicationTask : IStartupTask, IDisposable
    {
        public bool IsRunning { get; private set; }

        private volatile bool shouldPause;
        public int NumberOfActiveReplicationDestinations { get; private set; }
        public const int SystemDocsLimitForRemoteEtagUpdate = 15;
        public const int DestinationDocsLimitForRemoteEtagUpdate = 15;
        public const int EtlFilteredDocumentsForRemoteEtagUpdate = 15;

        public readonly ConcurrentSet<Task> activeTasks = new ConcurrentSet<Task>();

        private readonly ConcurrentDictionary<string, DestinationStats> destinationStats =
            new ConcurrentDictionary<string, DestinationStats>(StringComparer.OrdinalIgnoreCase);

        private DocumentDatabase docDb;
        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private bool firstTimeFoundNoReplicationDocument = true;
        private bool wrongReplicationSourceAlertSent;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> activeReplicationTasks = new ConcurrentDictionary<string, SemaphoreSlim>();

        private static readonly Task completedTask = Task.FromResult(false);

        private readonly ConcurrentDictionary<string, DateTime> destinationAlertSent = new ConcurrentDictionary<string, DateTime>();

        private readonly ConcurrentDictionary<string, bool> destinationForceBuffering = new ConcurrentDictionary<string, bool>();

        public ConcurrentDictionary<string, DestinationStats> DestinationStats => destinationStats;

        public ConcurrentDictionary<string, DateTime> Heartbeats => heartbeatDictionary;

        private int replicationAttempts;
        private int workCounter;
        private HttpRavenRequestFactory httpRavenRequestFactory;

        private IndependentBatchSizeAutoTuner autoTuner;
        private readonly ConcurrentDictionary<string, PrefetchingBehavior> prefetchingBehaviors = new ConcurrentDictionary<string, PrefetchingBehavior>();

        public IndexReplicationTask IndexReplication { get; set; }

        public TransformerReplicationTask TransformerReplication { get; set; }

        private CancellationTokenSource _cts;

        private int _needToPropogateToSiblings;
        private Timer _propagationTimeoutTimer;
        private ThreadLocal<bool> isThreadProcessingReplication = new ThreadLocal<bool>();

        public DatabaseIdsCache DatabaseIdsCache { get; private set; }

        private readonly ConcurrentDictionary<string, Etag> lastReplicatedDocumentOrTombstoneEtag = new ConcurrentDictionary<string, Etag>();
        private readonly ConcurrentDictionary<string, Etag> lastReplicatedAttachmentOrTombstoneEtag = new ConcurrentDictionary<string, Etag>();

        private bool _hasReplicationRanAtLeastOnce;

        /// <summary>
        /// Indicates that this thread is doing replication.
        /// </summary>
        internal ThreadLocal<bool> IsThreadProcessingReplication { get { return isThreadProcessingReplication; } set { isThreadProcessingReplication = value; } }

        public void Execute(DocumentDatabase database)
        {
            docDb = database;
            _transactionalStorageId = docDb.TransactionalStorage.Id.ToString();
            DatabaseIdsCache = new DatabaseIdsCache(database, log);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.WorkContext.CancellationToken);

            _propagationTimeoutTimer = docDb.TimerManager.NewTimer(state =>
            {
                if (Interlocked.CompareExchange(ref _needToPropogateToSiblings, 0, 1) == 1)
                    docDb.WorkContext.ReplicationResetEvent.Set();
            }, TimeSpan.FromSeconds(docDb.Configuration.Replication.ReplicationPropagationDelayInSeconds), TimeSpan.FromSeconds(docDb.Configuration.Replication.ReplicationPropagationDelayInSeconds));

            docDb.Notifications.OnIndexChange += (_, indexChangeNotification) =>
            {
                if (indexChangeNotification.Type == IndexChangeTypes.MapCompleted ||
                    indexChangeNotification.Type == IndexChangeTypes.ReduceCompleted ||
                    indexChangeNotification.Type == IndexChangeTypes.RemoveFromIndex
                    )
                    return;
                lastWorkIsIndexOrTransformer = true;
                docDb.WorkContext.ReplicationResetEvent.Set();
            };

            docDb.Notifications.OnTransformerChange += (_, __) =>
            {
                lastWorkIsIndexOrTransformer = true;
                docDb.WorkContext.ReplicationResetEvent.Set();
            };

            docDb.Notifications.OnAttachmentChange += (_, attachmentChangeNotification, ___) =>
            {
                if (IsThreadProcessingReplication.Value)
                {
                    Interlocked.Exchange(ref _needToPropogateToSiblings, 1);
                    return;
                }

                lastWorkIsIndexOrTransformer = false;

                //There is no need to be thread safe this is only used to prevent unnecessary work
                lastWorkAttachmentEtag = attachmentChangeNotification.Etag ?? Etag.InvalidEtag;
                docDb.WorkContext.ReplicationResetEvent.Set();
            };

            docDb.Notifications.OnBulkInsertChange += (_, BulkInsertChangeNotification) =>
            {
                lastWorkIsIndexOrTransformer = false;

                //There is no need to be thread safe this is only used to prevent unnecessary work
                lastWorkDocumentEtag = BulkInsertChangeNotification.Etag ?? Etag.InvalidEtag;
                docDb.WorkContext.ReplicationResetEvent.Set();
            };

            docDb.Notifications.OnDocumentChange += (_, dcn, ___) =>
            {
                if (dcn.Id.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) && // ignore sys docs
                                                                                       // but we do update for replication destination
                    string.Equals(dcn.Id, Constants.RavenReplicationDestinations, StringComparison.OrdinalIgnoreCase) == false &&
                    dcn.Id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false) // except for hilo documents
                    return;

                if (IsThreadProcessingReplication.Value)
                {
                    Interlocked.Exchange(ref _needToPropogateToSiblings, 1);
                    return;
                }

                lastWorkIsIndexOrTransformer = false;

                //There is no need to be thread safe this is only used to prevent unnecessary work
                lastWorkDocumentEtag = dcn.Etag ?? Etag.InvalidEtag;
                docDb.WorkContext.ReplicationResetEvent.Set();
            };

            var replicationRequestTimeoutInMs = docDb.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

            autoTuner = new IndependentBatchSizeAutoTuner(docDb.WorkContext, PrefetchingUser.Replicator);
            httpRavenRequestFactory = new HttpRavenRequestFactory
            {
                RequestTimeoutInMs = replicationRequestTimeoutInMs
            };

            var task = new Task(Execute, TaskCreationOptions.LongRunning);
            var disposableAction = new DisposableAction(task.Wait);
            // make sure that the doc db waits for the replication task shutdown
            docDb.ExtensionsState.GetOrAdd(Guid.NewGuid().ToString(), s => disposableAction);

            DeployIndexesAndTransformers(database);

            IndexReplication = new IndexReplicationTask(database, httpRavenRequestFactory, this);
            TransformerReplication = new TransformerReplicationTask(database, httpRavenRequestFactory, this);

            task.Start();

            IndexReplication.Start();
            TransformerReplication.Start();
        }

        private static void DeployIndexesAndTransformers(DocumentDatabase database)
        {
            try
            {
                var index = new RavenConflictDocuments();
                database.Indexes.PutIndex(index.IndexName, index.CreateIndexDefinition());
            }
            catch (Exception e)
            {
                log.ErrorException("Could not deploy 'Raven/ConflictDocuments' index.", e);
            }

            try
            {
                var transformer = new RavenConflictDocumentsTransformer();
                database.Transformers.PutTransform(transformer.TransformerName, transformer.CreateTransformerDefinition());
            }
            catch (Exception e)
            {
                log.ErrorException("Could not deploy 'Raven/ConflictDocumentsTransformer' transformer.", e);
            }
        }

        public void Pause()
        {
            shouldPause = true;
            if (log.IsDebugEnabled)
                log.Debug($"Replication task stopped for {docDb.Name}");
        }

        public void Continue()
        {
            shouldPause = false;
            if (log.IsDebugEnabled)
                log.Debug($"Replication task continued for {docDb.Name}");
        }

        private bool IsHotSpare()
        {
            if (docDb.RequestManager == null) return false;
            return docDb.RequestManager.IsInHotSpareMode;
        }

        private void Execute()
        {
            var sw = new Stopwatch();
            var name = GetType().Name;
            using (LogContext.WithResource(docDb.Name))
            {
                if (log.IsDebugEnabled)
                    log.Debug("Replication task started.");

                bool runningBecauseOfDataModifications = false;
                var context = docDb.WorkContext;
                NotifySiblings();
                var timeToWait = TimeSpan.FromMinutes(5);
                while (context.DoWork)
                {
                    IsRunning = !IsHotSpare() && !shouldPause;
                    if (log.IsDebugEnabled)
                        log.Debug("Replication task found work. Running: {0}", IsRunning);

                    if (IsRunning)
                    {
                        try
                        {
                            var copy = runningBecauseOfDataModifications;
                            AsyncHelpers.RunSync(() => ExecuteReplicationOnce(copy));
                        }
                        catch (Exception e)
                        {
                            log.ErrorException("Failed to perform replication", e);
                        }
                    }

                    runningBecauseOfDataModifications = docDb.WorkContext.ReplicationResetEvent.Wait(timeToWait);
                    docDb.WorkContext.ReplicationResetEvent.Reset();

                    timeToWait = runningBecauseOfDataModifications
                    ? TimeSpan.FromSeconds(30)
                    : TimeSpan.FromMinutes(5);
                }

                IsRunning = false;
            }
        }

        private bool HasAttachmentEtagChanged()
        {
            if (_hasReplicationRanAtLeastOnce == false)
                return true;

            var lastReplicatedAttachmentEtag = lastReplicatedAttachmentOrTombstoneEtag.Min(x => x.Value) ?? Etag.Empty;

            var lastTombstoneEtag = Etag.Empty;
            docDb.TransactionalStorage.Batch(actions =>
            {
                var lastTombstone = actions.Lists
                                           .ReadLast(Constants.RavenReplicationAttachmentsTombstones);

                if (lastTombstone != null) //no deletes --> no tombstones
                {
                    lastTombstoneEtag = lastTombstone.Etag;
                }
            });

            // since tombstones always come last in replication batches, 
            // if last tombstone etag is greater then last replicated etag, it is enough.
            if (EtagUtil.IsGreaterThan(lastTombstoneEtag, lastReplicatedAttachmentEtag))
                return true;

            var lastAttachmentEtag = Etag.Empty;
            docDb.TransactionalStorage.Batch(accessor =>
                lastAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag());

            return EtagUtil.IsGreaterThan(lastAttachmentEtag, lastReplicatedAttachmentEtag);
        }

        private bool HasDocumentOrTombstoneEtagChanged()
        {
            if (_hasReplicationRanAtLeastOnce == false)
                return true;

            var lastReplicatedDocumentEtag = lastReplicatedDocumentOrTombstoneEtag.Min(x => x.Value) ?? Etag.Empty;

            var lastTombstoneEtag = Etag.Empty;
            docDb.TransactionalStorage.Batch(actions =>
            {
                var lastTombstone = actions.Lists
                                           .ReadLast(Constants.RavenReplicationDocsTombstones);

                if (lastTombstone != null) //no deletes --> no tombstones
                {
                    lastTombstoneEtag = lastTombstone.Etag;
                }
            });

            // since tombstones always come last in replication batches, 
            // if last tombstone etag is greater then last replicated etag, it is enough.
            if (EtagUtil.IsGreaterThan(lastTombstoneEtag, lastReplicatedDocumentEtag))
                return true;

            var lastDocumentEtag = Etag.Empty;
            docDb.TransactionalStorage.Batch(accessor =>
                lastDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag());

            return EtagUtil.IsGreaterThan(lastDocumentEtag, lastReplicatedDocumentEtag);
        }

        public Task ExecuteReplicationOnce(bool runningBecauseOfDataModifications)
        {
            using (docDb.DisableAllTriggersForCurrentThread())
            {
                var destinations = GetReplicationDestinations();

                ClearCachedLastEtagForRemovedServers(destinations);

                NumberOfActiveReplicationDestinations = destinations.Length;

                if (destinations.Length == 0)
                {
                    WarnIfNoReplicationTargetsWereFound();
                    return completedTask;
                }

                var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);

                var destinationForReplication = destinations.Where(
                    dest =>
                    {
                        if (runningBecauseOfDataModifications == false)
                            return true;
                        return IsNotFailing(dest, currentReplicationAttempts);
                    }).ToList();

                CleanupPrefetchingBehaviors(destinations.Select(x => x.ConnectionStringOptions.Url),
                    destinations.Except(destinationForReplication).Select(x => x.ConnectionStringOptions.Url));

                var startedTasks = new List<Task>();

                foreach (var dest in destinationForReplication)
                {
                    DestinationStats stat;
                    // local copy for the replication task to use
                    var destUrl = dest.ConnectionStringOptions.Url;
                    // if last change is due to index or transformer we continue replicating normally.
                    // if we can't verify work needs to be done we asume there is work to be done.
                    var lastWorkNotificationIsIndexOrTransformer = lastWorkIsIndexOrTransformer;
                    if (lastWorkNotificationIsIndexOrTransformer == false && destinationStats.TryGetValue(dest.ConnectionStringOptions.Url, out stat))
                    {
                        // if we didn't get any work to do and the destination can't be stale for more than 5 minutes
                        // we will skip querying the destination for its last Etag until we get some real work.
                        if (stat.LastReplicatedEtag == lastWorkDocumentEtag && stat.LastReplicatedAttachmentEtag == lastWorkAttachmentEtag
                            && (SystemTime.UtcNow - (stat.LastSuccessTimestamp ?? DateTime.MinValue)).TotalMinutes <= 5)
                            continue;
                    }

                    var holder = activeReplicationTasks.GetOrAdd(dest.ConnectionStringOptions.Url, s => new SemaphoreSlim(1));
                    if (holder.Wait(0) == false)
                    {
                        if (log.IsDebugEnabled)
                        {
                            log.Debug("Replication to distination {0} skipped due to existing replication operation", dest.ConnectionStringOptions.Url);
                        }
                        continue;
                    }

                    var replicationTask = Task.Factory.StartNew(
                        state =>
                        {
                            ReplicationStrategy destination = (ReplicationStrategy)state;

                            using (LogContext.WithResource(docDb.Name))
                            using (CultureHelper.EnsureInvariantCulture())
                            {
                                try
                                {
                                    var lastDocumentEtag = Etag.InvalidEtag;
                                    var lastAttachmentEtag = Etag.InvalidEtag;
                                    _hasReplicationRanAtLeastOnce = true;

                                    try
                                    {
                                        if (ReplicateTo(destination, out lastDocumentEtag, out lastAttachmentEtag))
                                        {
                                            docDb.WorkContext.NotifyAboutWork();

                                            return new LastReplicatedEtags { LastReplicatedDocumentEtag = lastDocumentEtag, LastReplicatedAttachmentEtag = lastAttachmentEtag };
                                        }
                                    }
                                    finally
                                    {
                                        if (lastDocumentEtag != Etag.InvalidEtag)
                                            lastReplicatedDocumentOrTombstoneEtag[destUrl] = lastDocumentEtag;

                                        if (lastAttachmentEtag != Etag.InvalidEtag)
                                            lastReplicatedAttachmentOrTombstoneEtag[destUrl] = lastAttachmentEtag;
                                    }
                                }
                                catch (Exception e)
                                {
                                    log.ErrorException("Could not replicate to " + destination, e);
                                }
                                return null;
                            }
                        }, dest);

                    startedTasks.Add(replicationTask);

                    activeTasks.Add(replicationTask);
                    replicationTask.ContinueWith(
                        t =>
                        {
                            // here we purge all the completed tasks at the head of the queue
                            if (activeTasks.TryRemove(t) == false)
                            {
                                if (log.IsWarnEnabled)
                                {
                                    log.Warn("Failed to remove a replication task from active tasks, was it already removed?");
                                }
                            }

                            if (t.Result == null)
                                return;

                            docDb.TransactionalStorage.Batch(actions =>
                            {
                                var shouldRunAgain = (t.Result.LastReplicatedDocumentEtag != Etag.InvalidEtag &&
                                                      actions.Staleness.GetMostRecentDocumentEtag() != t.Result.LastReplicatedDocumentEtag) ||
                                                     (t.Result.LastReplicatedAttachmentEtag != Etag.InvalidEtag &&
                                                      actions.Staleness.GetMostRecentAttachmentEtag() != t.Result.LastReplicatedAttachmentEtag);

                                if (shouldRunAgain)
                                {
                                    docDb.WorkContext.ReplicationResetEvent.Set();
                                }
                            });

                        });
                }

                Task.WhenAll(startedTasks)
                    .ContinueWith(t =>
                    {
                        if (lastReplicatedDocumentOrTombstoneEtag.Count == 0 && lastReplicatedAttachmentOrTombstoneEtag.Count == 0)
                            return;

                        if (startedTasks.Any(st => st.IsFaulted) == false)
                        {
                            try
                            {
                                // purge tombstones that are not needed anymore
                                if (lastReplicatedDocumentOrTombstoneEtag.Count > 0)
                                {
                                    docDb.Maintenance.RemoveAllBefore(Constants.RavenReplicationDocsTombstones,
                                        lastReplicatedDocumentOrTombstoneEtag.Values.Min(), timeout: TimeSpan.FromSeconds(2));
                                }

                                if (lastReplicatedAttachmentOrTombstoneEtag.Count > 0)
                                {
                                    docDb.Maintenance.RemoveAllBefore(Constants.RavenReplicationAttachmentsTombstones,
                                        lastReplicatedAttachmentOrTombstoneEtag.Values.Min(), timeout: TimeSpan.FromSeconds(2));
                                }
                            }
                            catch (ConcurrencyException)
                            {
                                // we are ignoring concurrency exception this may happen if two batches compelete before
                                // the first batch finishes to cleanup the tombstones.
                            }
                            catch (Exception e)
                            {
                                log.ErrorException("Unexpected error during tombstones purging", e);
                            }
                        }
                        else
                        {
                            log.Error("One or more of replication (sub)tasks have failed, in this case I prefer not to delete tombstones.");
                            var faultedTasks = startedTasks.Where(stt => stt.IsFaulted).ToList();
                            for (int i = 0; i < faultedTasks.Count; i++)
                                log.ErrorException($"Task id={faultedTasks[i].Id} exception",
                                    faultedTasks[i].Exception.SimplifyException());
                        }
                    });

                if (startedTasks.Any() == false)
                    return completedTask;

                return Task.WhenAny(startedTasks.ToArray()).AssertNotFailed();
            }
        }

        private void ClearCachedLastEtagForRemovedServers(ReplicationStrategy[] destinations)
        {
            var serverUrls = destinations.Select(x => x.ConnectionStringOptions.Url).ToHashSet();

            var serversToRemove = new List<string>();
            foreach (var server in lastReplicatedDocumentOrTombstoneEtag)
            {
                if (serverUrls.Contains(server.Key))
                    continue;

                serversToRemove.Add(server.Key);
            }

            foreach (var serverUrl in serversToRemove)
            {
                Etag _;
                lastReplicatedDocumentOrTombstoneEtag.TryRemove(serverUrl, out _);
            }

            serversToRemove.Clear();
            foreach (var server in lastReplicatedAttachmentOrTombstoneEtag)
            {
                if (serverUrls.Contains(server.Key))
                    continue;

                serversToRemove.Add(server.Key);
            }

            foreach (var serverUrl in serversToRemove)
            {
                Etag _;
                lastReplicatedAttachmentOrTombstoneEtag.TryRemove(serverUrl, out _);
            }
        }

        /// <summary>
        /// this is intended for tests only, should not be used in production code
        /// </summary>
        public void ForceReplicationToRunOnce()
        {
            docDb.WorkContext.ReplicationResetEvent.Set();
        }

        private void CleanupPrefetchingBehaviors(IEnumerable<string> allDestinations, IEnumerable<string> failingDestinations)
        {
            PrefetchingBehavior prefetchingBehaviorToDispose;

            // remove prefetching behaviors for non-existing destinations
            foreach (var removedDestination in prefetchingBehaviors.Keys.Except(allDestinations))
            {
                if (prefetchingBehaviors.TryRemove(removedDestination, out prefetchingBehaviorToDispose))
                {
                    prefetchingBehaviorToDispose.Dispose();
                }
            }

            // also remove prefetchers if the destination is failing for a long time
            foreach (var failingDestination in failingDestinations)
            {
                DestinationStats stats;
                if (prefetchingBehaviors.ContainsKey(failingDestination) == false || destinationStats.TryGetValue(failingDestination, out stats) == false)
                    continue;

                if (stats.FirstFailureInCycleTimestamp != null && stats.LastFailureTimestamp != null &&
                    stats.LastFailureTimestamp - stats.FirstFailureInCycleTimestamp >= TimeSpan.FromMinutes(3))
                {
                    if (prefetchingBehaviors.TryRemove(failingDestination, out prefetchingBehaviorToDispose))
                    {
                        prefetchingBehaviorToDispose.Dispose();
                    }
                }
            }
        }

        private void NotifySiblings()
        {
            var notifications = new BlockingCollection<RavenConnectionStringOptions>();

            Task.Factory.StartNew(() => NotifySibling(notifications));

            int skip = 0;
            var replicationDestinations = GetReplicationDestinations();
            foreach (var replicationDestination in replicationDestinations)
            {
                notifications.TryAdd(replicationDestination.ConnectionStringOptions, 15 * 1000);
            }

            while (true)
            {
                int nextPageStart = skip; // will trigger rapid pagination
                var replicationSourceDocs = docDb.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, skip, 128, _cts.Token, ref nextPageStart);
                if (replicationSourceDocs.Length == 0)
                {
                    notifications.TryAdd(null, 15 * 1000); // marker to stop notify this
                    return;
                }

                skip += replicationSourceDocs.Length;

                foreach (var replicationSourceDoc in replicationSourceDocs)
                {
                    var sourceReplicationInformation = replicationSourceDoc.JsonDeserialization<SourceReplicationInformation>();
                    if (string.IsNullOrEmpty(sourceReplicationInformation.Source))
                        continue;

                    var match = replicationDestinations.FirstOrDefault(x =>
                                                           string.Equals(x.ConnectionStringOptions.Url,
                                                                         sourceReplicationInformation.Source,
                                                                         StringComparison.OrdinalIgnoreCase));

                    if (match != null)
                    {
                        notifications.TryAdd(match.ConnectionStringOptions, 15 * 1000);
                    }
                    else
                    {
                        notifications.TryAdd(new RavenConnectionStringOptions
                        {
                            Url = sourceReplicationInformation.Source
                        }, 15 * 1000);
                    }
                }
            }
        }

        private void NotifySibling(BlockingCollection<RavenConnectionStringOptions> collection)
        {
            using (LogContext.WithResource(docDb.Name))
                while (true)
                {
                    RavenConnectionStringOptions connectionStringOptions;
                    try
                    {
                        collection.TryTake(out connectionStringOptions, 15 * 1000, _cts.Token);
                        if (connectionStringOptions == null)
                            return;
                    }
                    catch (Exception e)
                    {
                        log.ErrorException("Could not get connection string options to notify sibling servers about restart", e);
                        return;
                    }

                    try
                    {
                        var url = connectionStringOptions.Url + "/replication/heartbeat?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;
                        var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, connectionStringOptions);
                        request.WebRequest.ContentLength = 0;
                        request.ExecuteRequest(_cts.Token);
                    }
                    catch (Exception e)
                    {
                        log.WarnException("Could not notify " + connectionStringOptions.Url + " about sibling server being up & running", e);
                    }
                }
        }



        private bool IsNotFailing(ReplicationStrategy dest, int currentReplicationAttempts)
        {
            var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(dest.ConnectionStringOptions.Url), null);
            if (jsonDocument == null)
                return true;
            var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();

            if (failureInformation.FailureCount > 1000)
            {
                var shouldReplicateTo = currentReplicationAttempts % 10 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Source = {3}; Failure count for {0} is {1}, skipping replication: {2}",
                        dest, failureInformation.FailureCount, shouldReplicateTo == false, docDb.Name + "@" + docDb.ServerUrl);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 100)
            {
                var shouldReplicateTo = currentReplicationAttempts % 5 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Source = {3}; Failure count for {0} is {1}, skipping replication: {2}",
                        dest, failureInformation.FailureCount, shouldReplicateTo == false, docDb.Name + "@" + docDb.ServerUrl);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 10)
            {
                var shouldReplicateTo = currentReplicationAttempts % 2 == 0;
                if (log.IsDebugEnabled)
                    log.Debug("Source = {3}; Failure count for {0} is {1}, skipping replication: {2}",
                        dest, failureInformation.FailureCount, shouldReplicateTo == false, docDb.Name + "@" + docDb.ServerUrl);
                return shouldReplicateTo;
            }
            return true;
        }

        public static string EscapeDestinationName(string url)
        {
            return Uri.EscapeDataString(url.Replace("https://", "").Replace("http://", "").Replace("/", "").Replace(":", ""));
        }

        private void WarnIfNoReplicationTargetsWereFound()
        {
            if (firstTimeFoundNoReplicationDocument)
            {
                firstTimeFoundNoReplicationDocument = false;
                log.Warn("Replication bundle is installed, but there is no destination in 'Raven/Replication/Destinations'.\r\nReplication results in NO-OP");
            }
        }

        private bool ReplicateTo(ReplicationStrategy destination,
            out Etag lastDocumentEtag,
            out Etag lastAttachmentEtag)
        {
            lastDocumentEtag = Etag.InvalidEtag;
            lastAttachmentEtag = Etag.InvalidEtag;
            try
            {
                if (docDb.Disposed)
                    return false;


                DestinationStats destStats = null;

                // We want to reduce the amount of immediately recurring replication in case of ongoing state of replication failure
                if (DestinationStats.TryGetValue(destination.ConnectionStringOptions.Url, out destStats))
                {
                    var cooldownBetweenFailures = TimeSpan.FromSeconds(1);
                    if (SystemTime.UtcNow - destStats.LastFailureTimestamp < cooldownBetweenFailures)
                        return false;
                }

                using (docDb.DisableAllTriggersForCurrentThread())
                using (var stats = new ReplicationStatisticsRecorder(destination, destinationStats))
                {
                    SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource;
                    using (var scope = stats.StartRecording("Destination"))
                    {
                        try
                        {
                            destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);
                            if (destinationsReplicationInformationForSource == null)
                            {
                                destinationsReplicationInformationForSource = GetLastReplicatedEtagFrom(destination);

                                if (destinationsReplicationInformationForSource == null)
                                {
                                    log.Error("Failed to replicate documents to destination {0}, because was not able to receive last Etag", destination.ConnectionStringOptions.Url);
                                    return false;
                                }
                            }

                            // update the last replicated etag to the one that we get from the destination server
                            lastDocumentEtag = destinationsReplicationInformationForSource.LastDocumentEtag;
                            lastAttachmentEtag = destinationsReplicationInformationForSource.LastAttachmentEtag;

                            if (destination.IsETL == false &&
                                destinationsReplicationInformationForSource.LastDocumentEtag == Etag.Empty &&
                                destinationsReplicationInformationForSource.LastAttachmentEtag == Etag.Empty)
                            {
                                IndexReplication.Execute();
                            }

                            scope.Record(RavenJObject.FromObject(destinationsReplicationInformationForSource));

                            if (destinationsReplicationInformationForSource.LastDocumentEtag == Etag.InvalidEtag &&
                                destinationsReplicationInformationForSource.LastAttachmentEtag == Etag.InvalidEtag &&
                                (destination.SpecifiedCollections == null || destination.SpecifiedCollections.Count == 0))
                            {
                                DateTime lastSent;

                                // todo: move lastModifiedDate after the condition
                                var lastModifiedDate = destinationsReplicationInformationForSource.LastModified.HasValue ? destinationsReplicationInformationForSource.LastModified.Value.ToLocalTime() : DateTime.MinValue;

                                if (destinationAlertSent.TryGetValue(destination.ConnectionStringOptions.Url, out lastSent) && (SystemTime.UtcNow - lastSent).TotalMinutes < 1)
                                {
                                    if (log.IsDebugEnabled) // todo: remove this log line
                                        log.Debug(string.Format(@"Destination server is forbidding replication due to a possibility of having multiple instances with same DatabaseId replicating to it. After 10 minutes from '{2}' another instance will start replicating. Destination Url: {0}. DatabaseId: {1}. Current source: {3}. Stored source on destination: {4}.", destination.ConnectionStringOptions.Url, docDb.TransactionalStorage.Id, lastModifiedDate, docDb.ServerUrl, destinationsReplicationInformationForSource.Source));
                                    return false;
                                }

                                docDb.AddAlert(new Alert
                                {
                                    AlertLevel = AlertLevel.Error,
                                    CreatedAt = SystemTime.UtcNow,
                                    Message = string.Format(@"Destination server is forbidding replication due to a possibility of having multiple instances with same DatabaseId replicating to it. After 10 minutes from '{2}' another instance will start replicating. Destination Url: {0}. DatabaseId: {1}. Current source: {3}. Stored source on destination: {4}.", destination.ConnectionStringOptions.Url, docDb.TransactionalStorage.Id, lastModifiedDate, docDb.ServerUrl, destinationsReplicationInformationForSource.Source),
                                    Title = $"Replication error. Multiple databases replicating at the same time with same DatabaseId ('{docDb.TransactionalStorage.Id}') detected.",
                                    UniqueKey = "Replication to " + destination.ConnectionStringOptions.Url + " errored. Wrong DatabaseId: " + docDb.TransactionalStorage.Id
                                });

                                destinationAlertSent.AddOrUpdate(destination.ConnectionStringOptions.Url, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);

                                return false;
                            }
                        }
                        catch (Exception e)
                        {
                            scope.RecordError(e);
                            log.WarnException("Failed to replicate to: " + destination, e);
                            return false;
                        }
                    }

                    bool? replicated = null;

                    int replicatedDocuments;

                    using (var scope = stats.StartRecording("Documents"))
                    {
                        switch (ReplicateDocuments(destination,
                            destinationsReplicationInformationForSource,
                            scope,
                            out replicatedDocuments,
                            out lastDocumentEtag))
                        {
                            case true:
                                replicated = true;
                                break;
                            case false:
                                return false;
                        }
                    }

                    using (var scope = stats.StartRecording("Attachments"))
                    {
                        if (destination.IsETL == false || destination.ReplicateAttachmentsInEtl)
                        {
                            switch (ReplicateAttachments(destination,
                                destinationsReplicationInformationForSource,
                                scope,
                                out lastAttachmentEtag))
                            {
                                case true:
                                    replicated = true;
                                    break;
                                case false:
                                    return false;
                            }
                        }
                    }

                    var elapsedMicroseconds = (long)(stats.ElapsedTime.Ticks * SystemTime.MicroSecPerTick);
                    docDb.WorkContext.MetricsCounters.GetReplicationDurationHistogram(destination).Update(elapsedMicroseconds);
                    UpdateReplicationPerformance(destination, stats.Started, stats.ElapsedTime, replicatedDocuments);

                    return replicated ?? false;
                }
            }
            finally
            {
                var holder = activeReplicationTasks.GetOrAdd(destination.ConnectionStringOptions.Url, s => new SemaphoreSlim(0, 1));
                holder.Release();
            }
        }

        private void UpdateReplicationPerformance(ReplicationStrategy destination, DateTime startTime, TimeSpan elapsed, int batchSize)
        {
            if (batchSize > 0)
            {
                var queue = docDb.WorkContext.MetricsCounters.GetReplicationPerformanceStats(destination);
                queue.Enqueue(new ReplicationPerformanceStats
                {
                    Duration = elapsed,
                    Started = startTime,
                    BatchSize = batchSize
                });

                while (queue.Count() > 25)
                {
                    ReplicationPerformanceStats _;
                    queue.TryDequeue(out _);
                }
            }
        }

        [Obsolete("Use RavenFS instead.")]
        private bool? ReplicateAttachments(ReplicationStrategy destination,
            SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource,
            ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder,
            out Etag lastAttachmentEtag)
        {
            Tuple<RavenJArray, Etag> tuple;
            RavenJArray attachments;
            lastAttachmentEtag = Etag.InvalidEtag;

            using (var scope = recorder.StartRecording("Get"))
            {
                tuple = GetAttachments(destinationsReplicationInformationForSource, destination, scope);
                attachments = tuple.Item1;

                if (attachments == null || attachments.Length == 0)
                {
                    if (tuple.Item2 != destinationsReplicationInformationForSource.LastAttachmentEtag)
                    {
                        SetLastReplicatedEtagForServer(destination, lastAttachmentEtag: tuple.Item2);
                    }
                    return null;
                }
            }

            using (var scope = recorder.StartRecording("Send"))
            {
                string lastError;
                if (TryReplicationAttachments(destination, attachments, out lastError) == false) // failed to replicate, start error handling strategy
                {
                    if (IsFirstFailure(destination.ConnectionStringOptions.Url))
                    {
                        log.Info("This is the first failure for {0}, assuming transient failure and trying again", destination);
                        if (TryReplicationAttachments(destination, attachments, out lastError)) // success on second fail
                        {
                            RecordSuccess(destination.ConnectionStringOptions.Url, lastReplicatedEtag: tuple.Item2, forDocuments: false);
                            lastAttachmentEtag = tuple.Item2;
                            return true;
                        }
                    }

                    scope.RecordError(lastError);
                    RecordFailure(destination.ConnectionStringOptions.Url, lastError);
                    return false;
                }
            }

            RecordSuccess(destination.ConnectionStringOptions.Url,
                lastReplicatedEtag: tuple.Item2, forDocuments: false);
            lastAttachmentEtag = tuple.Item2;

            return true;
        }

        private bool? ReplicateDocuments(ReplicationStrategy destination,
            SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource,
            ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope recorder,
            out int replicatedDocuments, out Etag lastReplicatedEtag)
        {
            replicatedDocuments = 0;
            JsonDocumentsToReplicate documentsToReplicate = null;
            var sp = Stopwatch.StartNew();
            IDisposable removeBatch = null;
            lastReplicatedEtag = Etag.InvalidEtag;

            var url = destination.ConnectionStringOptions.Url;
            var prefetchingBehavior = prefetchingBehaviors.GetOrAdd(url,
                x => docDb.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Replicator, autoTuner, $"Replication for URL: {destination.ConnectionStringOptions.Url}"));

            prefetchingBehavior.AdditionalInfo = $"For destination: {url}. Last replicated etag: {destinationsReplicationInformationForSource.LastDocumentEtag}";

            try
            {
                using (var scope = recorder.StartRecording("Get"))
                {
                    documentsToReplicate = GetJsonDocuments(destinationsReplicationInformationForSource, destination, prefetchingBehavior, scope);
                    if (documentsToReplicate.Documents == null || documentsToReplicate.Documents.Length == 0)
                    {
                        if (documentsToReplicate.LastEtag != destinationsReplicationInformationForSource.LastDocumentEtag)
                        {
                            // we don't notify remote server about updates to system docs, see: RavenDB-715
                            if (documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments == 0
                                || documentsToReplicate.CountOfFilteredDocumentsWhichAreSystemDocuments > SystemDocsLimitForRemoteEtagUpdate
                                || documentsToReplicate.CountOfFilteredDocumentsWhichOriginFromOtherDestinations > DestinationDocsLimitForRemoteEtagUpdate // see RavenDB-1555, RavenDB-4750
                                || documentsToReplicate.CountOfFilteredEtlDocuments > EtlFilteredDocumentsForRemoteEtagUpdate)
                            {
                                using (scope.StartRecording("Notify"))
                                {
                                    SetLastReplicatedEtagForServer(destination, lastDocEtag: documentsToReplicate.LastEtag);
                                    scope.Record(new RavenJObject
                                             {
                                                 { "LastDocEtag", documentsToReplicate.LastEtag.ToString() }
                                             });
                                }
                            }
                        }
                        RecordLastEtagChecked(url, documentsToReplicate.LastEtag);
                        return null;
                    }
                }

                // if the db is idling in all respect except sending out replication, let us keep it that way.
                docDb.WorkContext.UpdateFoundWork();

                removeBatch = prefetchingBehavior.UpdateCurrentlyUsedBatches(documentsToReplicate.LoadedDocs);

                using (var scope = recorder.StartRecording("Send"))
                {
                    string lastError;
                    if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError) == false) // failed to replicate, start error handling strategy
                    {
                        if (IsFirstFailure(url))
                        {
                            log.Info(
                                "This is the first failure for {0}, assuming transient failure and trying again",
                                destination);
                            if (TryReplicationDocuments(destination, documentsToReplicate.Documents, out lastError)) // success on second fail
                            {
                                RecordSuccess(url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
                                prefetchingBehavior.CleanupDocuments(documentsToReplicate.LastEtag);
                                lastReplicatedEtag = documentsToReplicate.LastEtag;
                                return true;
                            }
                        }
                        // if we had an error sending to this endpoint, it might be because we are sending too much data, or because
                        // the request timed out. This will let us know that the next time we try, we'll use just the initial doc counts
                        // and we'll be much more conservative with increasing the sizes
                        prefetchingBehavior.OutOfMemoryExceptionHappened();
                        scope.RecordError(lastError);
                        RecordFailure(url, lastError);
                        return false;
                    }
                }
            }
            finally
            {
                if (documentsToReplicate?.LoadedDocs != null)
                {
                    prefetchingBehavior.UpdateAutoThrottler(documentsToReplicate.LoadedDocs, sp.Elapsed);
                    replicatedDocuments = documentsToReplicate.LoadedDocs.Count;
                }

                removeBatch?.Dispose();
            }

            RecordSuccess(url, documentsToReplicate.LastEtag, documentsToReplicate.LastLastModified);
            prefetchingBehavior.CleanupDocuments(documentsToReplicate.LastEtag);
            lastReplicatedEtag = documentsToReplicate.LastEtag;
            return true;
        }

        private void SetLastReplicatedEtagForServer(ReplicationStrategy destination, Etag lastDocEtag = null, Etag lastAttachmentEtag = null)
        {
            try
            {
                var url = GetUrlFor(destination, "/replication/lastEtag");

                if (lastDocEtag != null)
                    url += "&docEtag=" + lastDocEtag;
                if (lastAttachmentEtag != null)
                    url += "&attachmentEtag=" + lastAttachmentEtag;

                var request = httpRavenRequestFactory.Create(url, HttpMethods.Put, destination.ConnectionStringOptions, GetRequestBuffering(destination));
                request.Write(new byte[0]);
                request.ExecuteRequest(_cts.Token);
                if (log.IsDebugEnabled)
                    log.Debug("Sent last replicated document Etag {0} to server {1}", lastDocEtag, destination.ConnectionStringOptions.Url);
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
                    log.WarnException("Replication is not enabled on: " + destination, e);
                else
                    log.WarnException("Failed to contact replication destination: " + destination, e);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to contact replication destination: " + destination, e);
            }
        }

        private string GetUrlFor(ReplicationStrategy destination, string endpoint)
        {
            var url = destination.ConnectionStringOptions.Url + endpoint + "?from=" + UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;

            if (destination.SpecifiedCollections == null || destination.SpecifiedCollections.Count == 0)
                return url;

            return url + ("&collections=" + string.Join(";", destination.SpecifiedCollections.Keys));
        }

        private void RecordFailure(string url, string lastError)
        {
            var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            var failureCount = Interlocked.Increment(ref stats.FailureCountInternal);
            stats.LastFailureTimestamp = SystemTime.UtcNow;

            if (stats.FirstFailureInCycleTimestamp == null)
                stats.FirstFailureInCycleTimestamp = SystemTime.UtcNow;

            if (string.IsNullOrWhiteSpace(lastError) == false)
                stats.LastError = lastError;

            var jsonDocument = docDb.Documents.Get(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null);
            var failureInformation = new DestinationFailureInformation { Destination = url };
            if (jsonDocument != null)
            {
                failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
                // we only want to update this once a minute, otherwise we have churn with starting replication
                // because we are writing a failure document
                if ((SystemTime.UtcNow - jsonDocument.LastModified.GetValueOrDefault()).TotalMinutes < 1)
                {
                    return;
                }
            }
            failureInformation.FailureCount = failureCount;

            docDb.Documents.Put(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null,
                RavenJObject.FromObject(failureInformation), new RavenJObject(), null);

        }

        private void RecordLastEtagChecked(string url, Etag lastEtagChecked)
        {
            var stats = destinationStats.GetOrDefault(url, new DestinationStats { Url = url });
            stats.LastEtagCheckedForReplication = lastEtagChecked;
        }

        private void RecordSuccess(string url,
            Etag lastReplicatedEtag = null, DateTime? lastReplicatedLastModified = null,
            DateTime? lastHeartbeatReceived = null, string lastError = null, bool forDocuments = true)
        {
            var stats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            Interlocked.Exchange(ref stats.FailureCountInternal, 0);
            stats.LastSuccessTimestamp = SystemTime.UtcNow;
            stats.FirstFailureInCycleTimestamp = null;

            if (lastReplicatedEtag != null)
            {
                stats.LastEtagCheckedForReplication = lastReplicatedEtag;
                if (forDocuments)
                    stats.LastReplicatedEtag = lastReplicatedEtag;
                else
                    stats.LastReplicatedAttachmentEtag = lastReplicatedEtag;
            }

            if (lastReplicatedLastModified.HasValue)
                stats.LastReplicatedLastModified = lastReplicatedLastModified;

            if (lastHeartbeatReceived.HasValue)
                stats.LastHeartbeatReceived = lastHeartbeatReceived;

            if (!string.IsNullOrWhiteSpace(lastError))
                stats.LastError = lastError;

            docDb.Documents.Delete(Constants.RavenReplicationDestinationsBasePath + EscapeDestinationName(url), null, null);

            TaskCompletionSource<object> result;
            while (_waitForReplicationTasks.TryDequeue(out result))
            {
                ThreadPool.QueueUserWorkItem(task => ((TaskCompletionSource<object>)task).TrySetResult(null), result);
            }
        }

        private bool IsFirstFailure(string url)
        {
            var destStats = destinationStats.GetOrAdd(url, new DestinationStats { Url = url });
            return destStats.FailureCount == 0;
        }

        [Obsolete("Use RavenFS instead.")]
        private bool TryReplicationAttachments(ReplicationStrategy destination, RavenJArray jsonAttachments, out string errorMessage)
        {
            try
            {
                var url = destination.ConnectionStringOptions.Url + "/replication/replicateAttachments?from=" +
                          UrlEncodedServerUrl() + "&dbid=" + docDb.TransactionalStorage.Id;

                var sp = Stopwatch.StartNew();
                using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
                {
                    var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, GetRequestBuffering(destination));
                    request.WebRequest.Headers["Topology-Id"] = docDb.ClusterManager?.Value?.Engine.CurrentTopology.TopologyId.ToString();
                    request.WriteBson(jsonAttachments);
                    request.ExecuteRequest(_cts.Token);
                    log.Info("Replicated {0} attachments to {1} in {2:#,#;;0} ms", jsonAttachments.Length, destination, sp.ElapsedMilliseconds);
                    errorMessage = "";
                    return true;
                }
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                if (response != null)
                {
                    try
                    {
                        using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
                        {
                            var error = streamReader.ReadToEnd();

                            var ravenJObject = RavenJObject.Parse(error);
                            log.WarnException("Replication to " + destination + " had failed\r\n" + ravenJObject.Value<string>("Error"), e);
                            errorMessage = error;
                            return false;
                        }
                    }
                    catch (Exception readResponseEx)
                    {
                        log.WarnException("Failed to read replication request error to " + destination + " destination from the response", readResponseEx);
                    }
                }

                log.WarnException("Replication to " + destination + " had failed", e);
                errorMessage = e.Message;
                return false;
            }
            catch (Exception e)
            {
                log.WarnException("Replication to " + destination + " had failed", e);
                errorMessage = e.Message;
                return false;
            }
        }

        private class WaitForReplicationState
        {
            public TaskCompletionSource<object> Task;
            public Etag Etag;
            public int Replicas;
            public TimeSpan Timeout;
            public DateTime Start;
        }

        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks = new ConcurrentQueue<TaskCompletionSource<object>>();

        public int GetSizeOfMajorityFromActiveReplicationDestination(int replicas)
        {
            var numberOfActiveReplicationDestinations = NumberOfActiveReplicationDestinations;
            var res = Math.Max(numberOfActiveReplicationDestinations / 2 + 1, replicas);
            return Math.Min(res, numberOfActiveReplicationDestinations);
        }

        public async Task<int> WaitForReplicationAsync(Etag etag, TimeSpan timeout, int numberOfReplicasToWaitFor)
        {

            var sp = Stopwatch.StartNew();
            while (true)
            {
                var waitForNextReplicationAsync = WaitForNextReplicationAsync();
                var past = ReplicatedPast(etag);
                if (past >= numberOfReplicasToWaitFor)
                    return past;

                var remaining = timeout - sp.Elapsed;
                //Times up return number of replicas so far
                if (remaining < TimeSpan.Zero)
                    return ReplicatedPast(etag);
                var nextTimeout = Task.Delay(remaining);
                try
                {

                    if (await Task.WhenAny(waitForNextReplicationAsync, nextTimeout).ConfigureAwait(false) == nextTimeout)
                    {
                        return ReplicatedPast(etag);
                    }
                }
                catch (OperationCanceledException)
                {
                    return ReplicatedPast(etag);
                }
            }
        }

        private Task WaitForNextReplicationAsync()
        {
            TaskCompletionSource<object> result;
            if (_waitForReplicationTasks.TryPeek(out result))
                return result.Task;

            result = new TaskCompletionSource<object>();
            _waitForReplicationTasks.Enqueue(result);
            return result.Task;
        }

        private int ReplicatedPast(Etag etag)
        {
            int replicated = 0;
            foreach (var dest in destinationStats.Values)
            {
                if (dest.LastReplicatedEtag?.CompareTo(etag) >= 0)
                {
                    replicated++;
                }
            }
            return replicated;
        }

        internal bool GetRequestBuffering(ReplicationStrategy destination)
        {
            return destinationForceBuffering.GetOrAdd(destination.ConnectionStringOptions.Url, docDb.Configuration.Replication.ForceReplicationRequestBuffering);
        }

        private bool TryReplicationDocuments(ReplicationStrategy destination, RavenJArray jsonDocuments, out string lastError)
        {
            try
            {
                if (log.IsDebugEnabled)
                    log.Debug("Starting to replicate {0} documents to {1}", jsonDocuments.Length, destination);

                var url = GetUrlFor(destination, "/replication/replicateDocs");

                url += $"&count={jsonDocuments.Length}";

                var sp = Stopwatch.StartNew();

                using (HttpRavenRequestFactory.Expect100Continue(destination.ConnectionStringOptions.Url))
                {
                    var request = httpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, GetRequestBuffering(destination));
                    request.WebRequest.Headers["Topology-Id"] = docDb.ClusterManager?.Value?.Engine.CurrentTopology.TopologyId.ToString();
                    request.Write(jsonDocuments);
                    request.ExecuteRequest(_cts.Token);

                    log.Info("Replicated {0} documents to {1} in {2:#,#;;0} ms", jsonDocuments.Length, destination, sp.ElapsedMilliseconds);
                    lastError = "";
                    return true;
                }
            }
            catch (WebException e)
            {
                HandleRequestBufferingErrors(e, destination);

                var response = e.Response as HttpWebResponse;
                var responseStream = response?.GetResponseStream();

                if (responseStream != null)
                {
                    try
                    {
                        using (var streamReader = new StreamReader(responseStream))
                        {
                            var error = streamReader.ReadToEnd();
                            log.WarnException("Replication to " + destination + " had failed\r\n" + error, e);
                            lastError = error;

                            return false;
                        }
                    }
                    catch (Exception readResponseEx)
                    {
                        log.WarnException("Failed to read replication request error to " + destination + " destination from the response", readResponseEx);
                    }
                }

                log.WarnException("Replication to " + destination + " had failed", e);
                lastError = e.Message;
                return false;
            }
            catch (Exception e)
            {
                log.WarnException("Replication to " + destination + " had failed", e);
                lastError = e.Message;
                return false;
            }
        }

        internal void HandleRequestBufferingErrors(Exception e, ReplicationStrategy destination)
        {
            if (destination.ConnectionStringOptions.Credentials != null && string.Equals(e.Message, "This request requires buffering data to succeed.", StringComparison.OrdinalIgnoreCase))
                destinationForceBuffering.AddOrUpdate(destination.ConnectionStringOptions.Url, true, (s, b) => true);
        }

        private class JsonDocumentsToReplicate
        {
            public Etag LastEtag { get; set; }
            public DateTime LastLastModified { get; set; }
            public RavenJArray Documents { get; set; }
            public int CountOfFilteredDocumentsWhichAreSystemDocuments { get; set; }
            public int CountOfFilteredDocumentsWhichOriginFromOtherDestinations { get; set; }
            public int CountOfFilteredEtlDocuments { get; set; }
            public List<JsonDocument> LoadedDocs { get; set; }
        }

        private string _transactionalStorageId;

        private JsonDocumentsToReplicate GetJsonDocuments(
            SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource,
            ReplicationStrategy destination,
            PrefetchingBehavior prefetchingBehavior,
            ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
        {
            var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
            var duration = Stopwatch.StartNew();
            var result = new JsonDocumentsToReplicate
            {
                LastEtag = Etag.Empty,
            };
            try
            {
                var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
                var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;
                var lastEtag = destinationsReplicationInformationForSource.LastDocumentEtag ?? Etag.Empty;

                int docsSinceLastReplEtag = 0;
                List<JsonDocument> fetchedDocs;
                List<JsonDocument> docsToReplicate;
                result.LastEtag = lastEtag ?? Etag.Empty;
                var isEtl = destination.IsETL;

                var collections = isEtl == false ? null :
                    new HashSet<string>(destination.SpecifiedCollections.Keys, StringComparer.OrdinalIgnoreCase);
                prefetchingBehavior.SetEntityNames(collections);

                while (true)
                {
                    _cts.Token.ThrowIfCancellationRequested();

                    fetchedDocs = GetDocsToReplicate(prefetchingBehavior,
                        result.LastEtag, maxNumberOfItemsToReceiveInSingleBatch);

                    IEnumerable<JsonDocument> handled = fetchedDocs;

                    foreach (var handler in new IReplicatedDocsHandler[]
                            {
                        new FilterReplicatedDocs(docDb.Documents, destination, prefetchingBehavior, destinationId, result.LastEtag),
                        new FilterAndTransformSpecifiedCollections(docDb, destination, destinationId)
                    })
                    {
                        handled = handler.Handle(handled);
                    }

                    docsToReplicate = handled.ToList();

                    docsSinceLastReplEtag += fetchedDocs.Count;
                    var filteredSystemDocuments = fetchedDocs.Count(doc => destination.IsSystemDocumentId(doc.Key));
                    result.CountOfFilteredDocumentsWhichAreSystemDocuments += filteredSystemDocuments;
                    var filteredOriginatedFromOtherDestinations = fetchedDocs
                        .Count(doc => destination.OriginatedAtOtherDestinations(_transactionalStorageId, doc.Metadata) &&
                                        !destination.IsSystemDocumentId(doc.Key));
                    result.CountOfFilteredDocumentsWhichOriginFromOtherDestinations += filteredOriginatedFromOtherDestinations;

                    if (isEtl)
                    {
                        var filteredDocuments = filteredSystemDocuments + filteredOriginatedFromOtherDestinations;
                        result.CountOfFilteredEtlDocuments += fetchedDocs.Count - docsToReplicate.Count - filteredDocuments;
                    }

                    if (fetchedDocs.Count > 0)
                    {
                        var lastDoc = fetchedDocs.Last();
                        Debug.Assert(lastDoc.Etag != null);
                        result.LastEtag = lastDoc.Etag;

                        if (lastDoc.LastModified.HasValue)
                            result.LastLastModified = lastDoc.LastModified.Value;
                    }

                    if (fetchedDocs.Count == 0 || docsToReplicate.Count != 0)
                    {
                        break;
                    }

                    if (log.IsDebugEnabled)
                        log.Debug("All the docs were filtered, trying another batch from etag [>{0}]", result.LastEtag);

                    if (duration.Elapsed > timeout)
                        break;
                }

                if (log.IsDebugEnabled)
                    log.Debug(() =>
                    {
                        if (docsSinceLastReplEtag == 0)
                            return string.Format("No documents to replicate to {0} - last replicated etag: {1}", destination,
                                lastEtag);

                        if (docsSinceLastReplEtag == docsToReplicate.Count)
                            return string.Format("Replicating {0} docs [>{1}] to {2}.",
                                docsSinceLastReplEtag,
                                lastEtag,
                                destination);

                        var diff = fetchedDocs.Except(docsToReplicate).Select(x => x.Key);
                        return string.Format("Replicating {1} docs (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
                            docsSinceLastReplEtag,
                                docsToReplicate.Count,
                            destination,
                            string.Join(", ", diff),
                            lastEtag);
                    });

                scope.Record(new RavenJObject
                {
                    {"StartEtag", lastEtag.ToString()},
                    {"EndEtag", result.LastEtag.ToString()},
                    {"Count", docsSinceLastReplEtag},
                    {"FilteredCount", docsToReplicate.Count}
                });

                result.LoadedDocs = docsToReplicate;
                docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeMetric(destination).Mark(docsSinceLastReplEtag);
                docDb.WorkContext.MetricsCounters.GetReplicationBatchSizeHistogram(destination).Update(docsSinceLastReplEtag);

                result.Documents = new RavenJArray(docsToReplicate
                    .Select(x =>
                    {
                        JsonDocument.EnsureIdInMetadata(x);
                        EnsureReplicationInformationInMetadata(x.Metadata, docDb);
                        return x;
                    })
                    .Select(x => x.ToJson()));
            }
            catch (Exception e)
            {
                scope.RecordError(e);
                log.WarnException(
                    "Could not get documents to replicate after: " +
                    destinationsReplicationInformationForSource.LastDocumentEtag, e);
            }
            return result;
        }

        private List<JsonDocument> GetDocsToReplicate(PrefetchingBehavior prefetchingBehavior, Etag from,
            int? maxNumberOfItemsToReceiveInSingleBatch)
        {
            var docsToReplicate = prefetchingBehavior.GetDocumentsBatchFrom(from, maxNumberOfItemsToReceiveInSingleBatch);
            Etag lastEtag = null;
            if (docsToReplicate.Count > 0)
                lastEtag = docsToReplicate[docsToReplicate.Count - 1].Etag;

            var maxNumberOfTombstones = Math.Max(1024, docsToReplicate.Count);
            List<JsonDocument> tombstones = null;
            docDb.TransactionalStorage.Batch(actions =>
            {
                tombstones = actions
                    .Lists
                    .Read(Constants.RavenReplicationDocsTombstones, from, lastEtag, maxNumberOfTombstones + 1)
                    .Where(x =>
                    {
                        if (x == null)
                            log.Warn("Couldn't get a tombstone from '{0}' list", Constants.RavenReplicationDocsTombstones);
                        return x != null;
                    })
                    .Select(x => new JsonDocument
                    {
                        Etag = x.Etag,
                        Key = x.Key,
                        Metadata = x.Data,
                        DataAsJson = new RavenJObject()
                    })
                    .ToList();
            });
            var results = docsToReplicate.Concat(tombstones);

            if (tombstones.Count >= maxNumberOfTombstones + 1)
            {
                var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
                log.Info("Replication batch trimmed. Found more than '{0}' document tombstones. Last etag from prefetcher: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

                results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
            }

            results = results.OrderBy(x => x.Etag);

            // can't return earlier, because we need to know if there are tombstones that need to be send
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

            return results.ToList();
        }

        [Obsolete("Use RavenFS instead.")]
        private Tuple<RavenJArray, Etag> GetAttachments(SourceReplicationInformationWithBatchInformation destinationsReplicationInformationForSource, ReplicationStrategy destination, ReplicationStatisticsRecorder.ReplicationStatisticsRecorderScope scope)
        {
            var timeout = TimeSpan.FromSeconds(docDb.Configuration.Replication.FetchingFromDiskTimeoutInSeconds);
            var duration = Stopwatch.StartNew();

            RavenJArray attachments = null;
            Etag lastAttachmentEtag = Etag.Empty;
            try
            {
                var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
                var maxNumberOfItemsToReceiveInSingleBatch = destinationsReplicationInformationForSource.MaxNumberOfItemsToReceiveInSingleBatch;

                docDb.TransactionalStorage.Batch(actions =>
                {
                    int attachmentSinceLastEtag = 0;
                    List<AttachmentInformation> attachmentsToReplicate;
                    List<AttachmentInformation> filteredAttachmentsToReplicate;
                    var startEtag = destinationsReplicationInformationForSource.LastAttachmentEtag;
                    lastAttachmentEtag = startEtag;
                    while (true)
                    {
                        attachmentsToReplicate = GetAttachmentsToReplicate(actions, lastAttachmentEtag, maxNumberOfItemsToReceiveInSingleBatch);

                        filteredAttachmentsToReplicate = attachmentsToReplicate.Where(attachment => destination.FilterAttachments(attachment, destinationId)).ToList();

                        attachmentSinceLastEtag += attachmentsToReplicate.Count;

                        if (attachmentsToReplicate.Count == 0 ||
                            filteredAttachmentsToReplicate.Count != 0)
                        {
                            break;
                        }

                        AttachmentInformation jsonDocument = attachmentsToReplicate.Last();
                        Etag attachmentEtag = jsonDocument.Etag;
                        if (log.IsDebugEnabled)
                            log.Debug("All the attachments were filtered, trying another batch from etag [>{0}]", attachmentEtag);
                        lastAttachmentEtag = attachmentEtag;

                        if (duration.Elapsed > timeout)
                            break;
                    }

                    if (log.IsDebugEnabled)
                        log.Debug(() =>
                        {
                            if (attachmentSinceLastEtag == 0)
                                return string.Format("No attachments to replicate to {0} - last replicated etag: {1}", destination,
                                                     destinationsReplicationInformationForSource.LastAttachmentEtag);

                            if (attachmentSinceLastEtag == filteredAttachmentsToReplicate.Count)
                                return string.Format("Replicating {0} attachments [>{1}] to {2}.",
                                                 attachmentSinceLastEtag,
                                                 destinationsReplicationInformationForSource.LastAttachmentEtag,
                                                 destination);

                            var diff = attachmentsToReplicate.Except(filteredAttachmentsToReplicate).Select(x => x.Key);
                            return string.Format("Replicating {1} attachments (out of {0}) [>{4}] to {2}. [Not replicated: {3}]",
                                                 attachmentSinceLastEtag,
                                                 filteredAttachmentsToReplicate.Count,
                                                 destination,
                                                 string.Join(", ", diff),
                                                 destinationsReplicationInformationForSource.LastAttachmentEtag);
                        });

                    scope.Record(new RavenJObject
                                 {
                                     {"StartEtag", startEtag.ToString()},
                                     {"EndEtag", lastAttachmentEtag.ToString()},
                                     {"Count", attachmentSinceLastEtag},
                                     {"FilteredCount", filteredAttachmentsToReplicate.Count}
                                 });

                    attachments = new RavenJArray(filteredAttachmentsToReplicate
                                                      .Select(x =>
                                                      {
                                                          var data = new byte[0];
                                                          if (x.Size > 0)
                                                          {
                                                              data = actions.Attachments.GetAttachment(x.Key).Data().ReadData();
                                                          }

                                                          EnsureReplicationInformationInMetadata(x.Metadata, docDb);

                                                          return new RavenJObject
                                                                       {
                                                                           {"@metadata", x.Metadata},
                                                                           {"@id", x.Key},
                                                                           {"@etag", x.Etag.ToByteArray()},
                                                                           {"data", data}
                                                                       };
                                                      }));
                });
            }
            catch (InvalidDataException e)
            {
                RecordFailure(url: String.Empty, lastError: $"Data is corrupted, could not proceed with attachment replication. Exception : {e}");
                scope.RecordError(e);
                log.ErrorException("Data is corrupted, could not proceed with replication", e);
            }
            catch (Exception e)
            {
                log.WarnException("Could not get attachments to replicate after: " + destinationsReplicationInformationForSource.LastAttachmentEtag, e);
            }
            return Tuple.Create(attachments, lastAttachmentEtag);
        }

        [Obsolete("Use RavenFS instead.")]
        private List<AttachmentInformation> GetAttachmentsToReplicate(IStorageActionsAccessor actions, Etag lastAttachmentEtag, int? maxNumberOfItemsToReceiveInSingleBatch)
        {
            var maxNumberOfAttachments = 100;
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                maxNumberOfAttachments = Math.Min(maxNumberOfAttachments, maxNumberOfItemsToReceiveInSingleBatch.Value);

            var attachmentInformations = actions.Attachments.GetAttachmentsAfter(lastAttachmentEtag, maxNumberOfAttachments, 1024 * 1024 * 10).ToList();

            Etag lastEtag = null;
            if (attachmentInformations.Count > 0)
                lastEtag = attachmentInformations[attachmentInformations.Count - 1].Etag;

            var maxNumberOfTombstones = Math.Max(maxNumberOfAttachments, attachmentInformations.Count);
            var tombstones = actions
                .Lists
                .Read(Constants.RavenReplicationAttachmentsTombstones, lastAttachmentEtag, lastEtag, maxNumberOfTombstones + 1)
                            .Select(x => new AttachmentInformation
                            {
                                Key = x.Key,
                                Etag = x.Etag,
                                Metadata = x.Data,
                                Size = 0,
                            })
                .ToList();

            var results = attachmentInformations.Concat(tombstones);

            if (tombstones.Count >= maxNumberOfTombstones + 1)
            {
                var lastTombstoneEtag = tombstones[tombstones.Count - 1].Etag;
                log.Info("Replication batch trimmed. Found more than '{0}' attachment tombstones. Last attachment etag: '{1}'. Last tombstone etag: '{2}'.", maxNumberOfTombstones, lastEtag, lastTombstoneEtag);

                results = results.Where(x => EtagUtil.IsGreaterThan(x.Etag, lastTombstoneEtag) == false);
            }

            results = results.OrderBy(x => x.Etag);

            // can't return earlier, because we need to know if there are tombstones that need to be send
            if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                results = results.Take(maxNumberOfItemsToReceiveInSingleBatch.Value);

            return results.ToList();
        }

        internal SourceReplicationInformationWithBatchInformation GetLastReplicatedEtagFrom(ReplicationStrategy destination)
        {
            try
            {
                Etag currentEtag = Etag.Empty;
                docDb.TransactionalStorage.Batch(accessor => currentEtag = accessor.Staleness.GetMostRecentDocumentEtag());
                var url = GetUrlFor(destination, "/replication/lastEtag");

                url += "&currentEtag=" + currentEtag;

                var request = httpRavenRequestFactory.Create(url, HttpMethods.Get, destination.ConnectionStringOptions);
                var lastReplicatedEtagFrom = request.ExecuteRequest<SourceReplicationInformationWithBatchInformation>();
                if (log.IsDebugEnabled)
                    log.Debug("Received last replicated document Etag {0} from server {1}", lastReplicatedEtagFrom.LastDocumentEtag, destination.ConnectionStringOptions.Url);

                DatabaseIdsCache.Add(destination.ConnectionStringOptions.Url, lastReplicatedEtagFrom.DatabaseId);

                return lastReplicatedEtagFrom;
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
                    log.WarnException("Replication is not enabled on: " + destination, e);
                else
                    log.WarnException("Failed to contact replication destination: " + destination, e);
                RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to contact replication destination: " + destination, e);
                RecordFailure(destination.ConnectionStringOptions.Url, e.Message);
            }

            return null;
        }

        private string UrlEncodedServerUrl()
        {
            return Uri.EscapeDataString(docDb.ServerUrl);
        }

        internal ReplicationStrategy[] GetReplicationDestinations(Predicate<ReplicationDestination> predicate = null)
        {
            ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument;
            try
            {
                configurationDocument = docDb.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            }
            catch (Exception e)
            {
                log.Warn("Cannot get replication destinations", e);
                return new ReplicationStrategy[0];
            }

            if (configurationDocument == null)
            {
                return new ReplicationStrategy[0];
            }

            var replicationDocument = configurationDocument.MergedDocument;

            if (configurationDocument.LocalExists && string.IsNullOrWhiteSpace(replicationDocument.Source))
            {
                replicationDocument.Source = docDb.TransactionalStorage.Id.ToString();
                try
                {
                    var ravenJObject = RavenJObject.FromObject(replicationDocument);
                    ravenJObject.Remove("Id");
                    docDb.Documents.Put(Constants.RavenReplicationDestinations, configurationDocument.Etag, ravenJObject, configurationDocument.Metadata, null);
                }
                catch (ConcurrencyException)
                {
                    // we will get it next time
                }
            }

            if (replicationDocument.Source != docDb.TransactionalStorage.Id.ToString())
            {
                if (wrongReplicationSourceAlertSent == false)
                {
                    var dbName = string.IsNullOrEmpty(docDb.Name) ? "<system>" : docDb.Name;

                    docDb.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = SystemTime.UtcNow,
                        Message = "Source of the ReplicationDestinations document is not the same as the database it is located in",
                        Title = "Wrong replication source: " + replicationDocument.Source + " instead of " + docDb.TransactionalStorage.Id + " in database " + dbName,
                        UniqueKey = "Wrong source: " + replicationDocument.Source + ", " + docDb.TransactionalStorage.Id
                    });

                    wrongReplicationSourceAlertSent = true;
                }

                return new ReplicationStrategy[0];
            }

            wrongReplicationSourceAlertSent = false;

            return replicationDocument
                .Destinations
                .Where(x => !x.Disabled)
                .Where(x => predicate == null || predicate(x))
                .Select(GetConnectionOptionsSafe)
                .Where(x => x != null)
                .ToArray();
        }

        private ReplicationStrategy GetConnectionOptionsSafe(ReplicationDestination x)
        {
            try
            {
                return GetConnectionOptions(x, docDb);
            }
            catch (Exception e)
            {
                log.ErrorException(
                    string.Format("IGNORING BAD REPLICATION CONFIG!{0}Could not figure out connection options for [Url: {1}, ClientVisibleUrl: {2}]",
                    Environment.NewLine, x.Url, x.ClientVisibleUrl),
                    e);

                return null;
            }
        }

        public static ReplicationStrategy GetConnectionOptions(ReplicationDestination destination, DocumentDatabase database)
        {
            var replicationStrategy = new ReplicationStrategy
            {
                ReplicationOptionsBehavior = destination.TransitiveReplicationBehavior,
                CurrentDatabaseId = database.TransactionalStorage.Id.ToString()
            };
            return CreateReplicationStrategyFromDocument(destination, replicationStrategy);
        }

        private static ReplicationStrategy CreateReplicationStrategyFromDocument(ReplicationDestination destination, ReplicationStrategy replicationStrategy)
        {
            var url = destination.Url;
            if (string.IsNullOrEmpty(destination.Database) == false)
            {
                url = url + "/databases/" + destination.Database;
            }

            replicationStrategy.ConnectionStringOptions = new RavenConnectionStringOptions
            {
                Url = url,
                AuthenticationScheme = destination.AuthenticationScheme,
                ApiKey = destination.ApiKey,
                DefaultDatabase = destination.Database
            };

            if (destination.SpecifiedCollections != null)
            {
                replicationStrategy.SpecifiedCollections = new Dictionary<string, string>(destination.SpecifiedCollections, StringComparer.OrdinalIgnoreCase);
            }

            replicationStrategy.ReplicateAttachmentsInEtl = destination.ReplicateAttachmentsInEtl;

            if (string.IsNullOrEmpty(destination.Username) == false)
            {
                replicationStrategy.ConnectionStringOptions.Credentials = string.IsNullOrEmpty(destination.Domain)
                    ? new NetworkCredential(destination.Username, destination.Password)
                    : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
            }

            return replicationStrategy;
        }

        public void HandleHeartbeat(string src, bool wake)
        {
            ResetFailureForHeartbeat(src);

            heartbeatDictionary.AddOrUpdate(src, SystemTime.UtcNow, (_, __) => SystemTime.UtcNow);

            if (wake)
                docDb.WorkContext.ReplicationResetEvent.Set();
        }

        public bool IsHeartbeatAvailable(string src, DateTime lastCheck)
        {
            if (heartbeatDictionary.ContainsKey(src))
            {
                DateTime lastHeartbeat;
                if (heartbeatDictionary.TryGetValue(src, out lastHeartbeat))
                {
                    return lastHeartbeat >= lastCheck;
                }
            }

            return false;
        }


        private void ResetFailureForHeartbeat(string src)
        {
            RecordSuccess(src, lastHeartbeatReceived: SystemTime.UtcNow);
            docDb.WorkContext.ShouldNotifyAboutWork(() => "Replication Heartbeat from " + src);
            docDb.WorkContext.NotifyAboutWork();
        }

        public void Dispose()
        {
            DatabaseIdsCache.Dispose();

            if (_propagationTimeoutTimer != null)
            {
                docDb.TimerManager.ReleaseTimer(_propagationTimeoutTimer);
                _propagationTimeoutTimer = null;
            }

            if (IndexReplication != null)
                IndexReplication.Dispose();

            if (TransformerReplication != null)
                TransformerReplication.Dispose();

            _cts.Cancel();

            foreach (var activeTask in activeTasks)
            {
                try
                {
                    activeTask.Wait();
                }
                catch (OperationCanceledException)
                {
                    // okay
                }
                catch (Exception e)
                {
                    log.InfoException("Error while waiting for replication tasks to complete during replication disposal", e);
                }
            }

            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                prefetchingBehavior.Value.Dispose();
            }
        }

        private readonly ConcurrentDictionary<string, DateTime> heartbeatDictionary = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        private Etag lastWorkDocumentEtag;
        private Etag lastWorkAttachmentEtag;
        private bool lastWorkIsIndexOrTransformer;

        internal static void EnsureReplicationInformationInMetadata(RavenJObject metadata, DocumentDatabase database)
        {
            Debug.Assert(database != null);

            if (metadata == null)
                return;

            if (metadata.ContainsKey(Constants.RavenReplicationSource))
                return;

            metadata[Constants.RavenReplicationHistory] = new RavenJArray();
            metadata[Constants.RavenReplicationVersion] = 0;
            metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(database.TransactionalStorage.Id);
        }
    }

    internal class LastReplicatedEtags
    {
        public Etag LastReplicatedDocumentEtag { get; set; }
        public Etag LastReplicatedAttachmentEtag { get; set; }
    }

    internal class ReplicationStatisticsRecorder : IDisposable
    {
        private readonly ReplicationStrategy destination;

        private readonly ConcurrentDictionary<string, DestinationStats> destinationStats;

        private readonly RavenJObject record;

        private readonly RavenJArray records;

        private readonly Stopwatch watch;

        public ReplicationStatisticsRecorder(ReplicationStrategy destination, ConcurrentDictionary<string, DestinationStats> destinationStats)
        {
            this.destination = destination;
            this.destinationStats = destinationStats;
            watch = Stopwatch.StartNew();
            Started = SystemTime.UtcNow;
            records = new RavenJArray();
            record = new RavenJObject
                     {
                         { "Url", destination.ConnectionStringOptions.Url },
                         { "StartTime", SystemTime.UtcNow},
                         { "Records", records }
                     };
        }

        public DateTime Started { get; private set; }


        public TimeSpan ElapsedTime
        {
            get
            {
                return watch.Elapsed;
            }
        }

        public void Dispose()
        {
            record.Add("TotalExecutionTime", watch.Elapsed.ToString());

            var stats = destinationStats.GetOrDefault(destination.ConnectionStringOptions.Url, new DestinationStats { Url = destination.ConnectionStringOptions.Url });

            stats.LastStats.Insert(0, record);

            while (stats.LastStats.Length > 50)
                stats.LastStats.RemoveAt(stats.LastStats.Length - 1);
        }

        public ReplicationStatisticsRecorderScope StartRecording(string name)
        {
            var scopeRecord = new RavenJObject();
            records.Add(scopeRecord);
            return new ReplicationStatisticsRecorderScope(name, scopeRecord);
        }

        internal class ReplicationStatisticsRecorderScope : IDisposable
        {
            private readonly RavenJObject record;

            private readonly RavenJArray records;

            private readonly Stopwatch watch;

            public ReplicationStatisticsRecorderScope(string name, RavenJObject record)
            {
                this.record = record;
                records = new RavenJArray();

                record.Add("Name", name);
                record.Add("Records", records);

                watch = Stopwatch.StartNew();
            }

            public void Dispose()
            {
                record.Add("ExecutionTime", watch.Elapsed.ToString());
            }

            public void Record(RavenJObject value)
            {
                records.Add(value);
            }

            public void RecordError(Exception exception)
            {
                records.Add(new RavenJObject
                            {
                                { "Error", new RavenJObject
                                           {
                                               { "Type", exception.GetType().Name },
                                               { "Message", exception.Message }
                                           } }
                            });
            }

            public void RecordError(string error)
            {
                records.Add(new RavenJObject
                            {
                                { "Error", error }
                            });
            }

            public ReplicationStatisticsRecorderScope StartRecording(string name)
            {
                var scopeRecord = new RavenJObject();
                records.Add(scopeRecord);
                return new ReplicationStatisticsRecorderScope(name, scopeRecord);
            }

        }

    }

}
