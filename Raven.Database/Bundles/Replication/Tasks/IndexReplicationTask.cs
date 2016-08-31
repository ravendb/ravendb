// -----------------------------------------------------------------------
//  <copyright file="IndexReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Impl;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Bundles.Replication.Tasks
{
    public class IndexReplicationTask : ReplicationTaskBase
    {
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();

        private readonly TimeSpan replicationFrequency;
        private readonly TimeSpan lastQueriedFrequency;
        private readonly object indexReplicationLock = new object();
        private Timer indexReplicationTimer;
        private Timer lastQueriedTimer;

        private readonly ConcurrentDictionary<int, IndexToAdd> sideBySideIndexesToReplicate =
            new ConcurrentDictionary<int, IndexToAdd>();
        private readonly ConcurrentDictionary<string, ConcurrentSet<int>> replicatedSideBySideIndexIds = 
            new ConcurrentDictionary<string, ConcurrentSet<int>>();
        private long sideBySideWorkCounter;
        private readonly InterlockedLock interlockedLock = new InterlockedLock();

        public TimeSpan TimeToWaitBeforeSendingDeletesOfIndexesToSiblings { get; set; }

        public IndexReplicationTask(DocumentDatabase database, HttpRavenRequestFactory httpRavenRequestFactory, ReplicationTask replication)
            : base(database, httpRavenRequestFactory, replication)
        {
            replicationFrequency = TimeSpan.FromSeconds(database.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
            lastQueriedFrequency = TimeSpan.FromSeconds(database.Configuration.TimeToWaitBeforeRunningIdleIndexes.TotalSeconds / 2);
            TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.FromMinutes(1);
        }

        public void Start()
        {
            var indexDefinitions = Database.Indexes.Definitions;
            var sideBySideIndexes = indexDefinitions.Where(x => x.Name.StartsWith(Constants.SideBySideIndexNamePrefix)).ToList();
            foreach (var indexDefinition in sideBySideIndexes)
            {
                var indexName = indexDefinition.Name;
                var instance = Database.IndexStorage.GetIndexInstance(indexName);
                if (instance == null)
                {
                    //probably deleted
                    continue;
                }
                    
                var indexToAdd = new IndexToAdd
                {
                    Name = indexDefinition.Name,
                    Definition = indexDefinition,
                    Priority = instance.Priority
                };

                SetIndexReplaceInfo(indexName, indexToAdd);

                StartReplicatingSideBySideIndexAsync(indexToAdd);
            }

            Database.Notifications.OnIndexChange += OnIndexChange;
            Database.Notifications.OnDocumentChange += OnDocumentChange;
            
            indexReplicationTimer = Database.TimerManager.NewTimer(x => Execute(), TimeSpan.Zero, replicationFrequency);
            lastQueriedTimer = Database.TimerManager.NewTimer(x => SendLastQueried(), TimeSpan.Zero, lastQueriedFrequency);
        }

        public void StartReplicatingSideBySideIndexAsync(IndexToAdd indexToAdd)
        {
            if (indexToAdd.Name.StartsWith(Constants.SideBySideIndexNamePrefix))
                indexToAdd.Name = indexToAdd.Name.Substring(Constants.SideBySideIndexNamePrefix.Length);

            sideBySideIndexesToReplicate.TryAdd(indexToAdd.Definition.IndexId, indexToAdd);

            ReplicateSideBySideIndexes();
        }

        public void ReplicateSideBySideIndexes()
        {
            var currentWorkCount = Interlocked.Increment(ref sideBySideWorkCounter);

            if (interlockedLock.TryEnter() == false)
            {
                //already replicating side by side indexes
                return;
            }

            if (sideBySideIndexesToReplicate.Count == 0)
            {
                //nothing to do
                interlockedLock.Exit();
                return;
            }
            
            Task.Run(() =>
            {
                try
                {
                    var failedToReplicate = false;
                    var commonReplicatedIndexIds = new HashSet<int>();
                    var replicationDestinations = GetReplicationDestinations();
                    
                    foreach (var destination in replicationDestinations)
                    {
                        var url = destination.ConnectionStringOptions.Url;

                        var replicatedIndexIds = replicatedSideBySideIndexIds.GetOrAdd(url, new ConcurrentSet<int>());
                        var indexesToReplicate = sideBySideIndexesToReplicate
                            .Where(x => replicatedIndexIds.Contains(x.Key) == false).ToList();

                        if (indexesToReplicate.Count == 0)
                            continue;

                        try
                        {
                            ReplicateSideBySideIndexesMultiPut(destination, indexesToReplicate.Select(x => x.Value).ToList());
                            foreach (var index in indexesToReplicate)
                            {
                                replicatedIndexIds.TryAdd(index.Key);
                            }

                            if (commonReplicatedIndexIds.Count == 0)
                            {
                                commonReplicatedIndexIds.UnionWith(replicatedIndexIds);
                            }
                            else
                            {
                                commonReplicatedIndexIds.IntersectWith(replicatedIndexIds);
                            }
                        }
                        catch (Exception e)
                        {
                            failedToReplicate = true;
                            Log.ErrorException("Failed to replicate side by side index to " + destination, e);
                        }
                    }

                    if (failedToReplicate)
                        return;

                    //since we replicated the side by side indexes for all destinations,
                    //there won't be any need to replicate them again
                    foreach (var indexId in commonReplicatedIndexIds)
                    {
                        IndexToAdd _;
                        sideBySideIndexesToReplicate.TryRemove(indexId, out _);
                    }
                }
                catch (Exception e)
                {
                    Log.ErrorException("Failed to replicate side by side indexes", e);
                }
                finally
                {
                    interlockedLock.Exit();

                    var newCount = Interlocked.Read(ref sideBySideWorkCounter);
                    if (currentWorkCount != newCount)
                    {
                        //handling a race condition where we get a new side by side index to replicate
                        //and exiting while thinking that this index will be handled
                        ReplicateSideBySideIndexes();
                    }
                }
            }, Database.WorkContext.CancellationToken);
        }

        private void OnDocumentChange(DocumentDatabase db, DocumentChangeNotification notification, RavenJObject doc)
        {
            var docId = notification.Id;
            if (docId == null)
                return;

            if (docId.StartsWith(Constants.IndexReplacePrefix, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (notification.Type != DocumentChangeTypes.Put)
                return;

            var replaceIndexName = docId.Substring(Constants.IndexReplacePrefix.Length);
            var instance = Database.IndexStorage.GetIndexInstance(replaceIndexName);
            if (instance == null)
                return;

            var indexDefinition = Database.Indexes.GetIndexDefinition(replaceIndexName);
            if (indexDefinition == null)
                return;

            var indexToAdd = new IndexToAdd
            {
                Name = replaceIndexName,
                Definition = indexDefinition,
                Priority = instance.Priority,
            };

            SetIndexReplaceInfo(replaceIndexName, indexToAdd);

            StartReplicatingSideBySideIndexAsync(indexToAdd);
        }

        private void SetIndexReplaceInfo(string indexName, IndexToAdd indexToAdd)
        {
            var key = Constants.IndexReplacePrefix + indexName;
            var replaceDoc = Database.Documents.Get(key, null);
            if (replaceDoc == null)
                return;

            try
            {
                var indexReplaceInformation = replaceDoc.DataAsJson.JsonDeserialization<IndexReplaceDocument>();
                indexToAdd.MinimumEtagBeforeReplace = indexReplaceInformation.MinimumEtagBeforeReplace;
                indexToAdd.ReplaceTimeUtc = indexReplaceInformation.ReplaceTimeUtc;
            }
            catch (Exception)
            {
                //the index was already replaced or the document was deleted
                //anyway, we'll try to replicate this index with default settings
            }
        }

        public bool Execute(Func<ReplicationDestination, bool> shouldSkipDestinationPredicate = null)
        {
            if (Database.Disposed)
                return false;

            if (Monitor.TryEnter(indexReplicationLock) == false)
                return false;

            if (sideBySideIndexesToReplicate.Count > 0)
            {
                //we do replicate side by side indexes on creation
                //this handles the case when we aren't able to connect to a certain destination
                //we need to retry until we succeed
                ReplicateSideBySideIndexes();
            }

            try
            {
                using (CultureHelper.EnsureInvariantCulture())
                {
                    shouldSkipDestinationPredicate = shouldSkipDestinationPredicate ?? (x => x.SkipIndexReplication == false);
                    var replicationDestinations = GetReplicationDestinations(x => shouldSkipDestinationPredicate(x));

                    foreach (var destination in replicationDestinations)
                    {
                        try
                        {
                            var now = SystemTime.UtcNow;

                            var indexTombstones = GetTombstones(Constants.RavenReplicationIndexesTombstones, 0, 64,
                                // we don't send out deletions immediately, we wait for a bit
                                // to make sure that the user didn't reset the index or delete / create
                                // things manually
                                x => (now - x.CreatedAt) >= TimeToWaitBeforeSendingDeletesOfIndexesToSiblings);

                            var replicatedIndexTombstones = new Dictionary<string, int>();

                            ReplicateIndexDeletionIfNeeded(indexTombstones, destination, replicatedIndexTombstones);

                            var indexesToAdd = new List<IndexToAdd>();

                            var indexDefinitions = Database.Indexes.Definitions;
                            if (indexDefinitions.Length > 0)
                            {
                                var replicatedIds = replicatedSideBySideIndexIds.GetOrAdd(destination.ConnectionStringOptions.Url, new ConcurrentSet<int>());

                                var sideBySideIndexNamesToReplicate = indexDefinitions
                                    .Where(x => replicatedIds.Contains(x.IndexId) == false &&
                                                x.Name.StartsWith(Constants.SideBySideIndexNamePrefix))
                                    .Select(x => x.Name).ToList();

                                //filtering system indexes like Raven/DocumentsByEntityName and Raven/ConflictDocuments and side by side indexes
                                var indexesToReplicate = indexDefinitions.Where(x =>
                                    x.Name.StartsWith(Constants.SideBySideIndexNamePrefix) == false
                                    && x.Name.StartsWith("Raven/") == false);

                                foreach (var indexDefinition in indexesToReplicate)
                                {
                                    if (sideBySideIndexNamesToReplicate.Contains(Constants.SideBySideIndexNamePrefix + indexDefinition.Name))
                                    {
                                        //the side by side index for this index wasn't replicated,
                                        //no need to replicate the original index yet
                                        continue;
                                    }

                                    var instance = Database.IndexStorage.GetIndexInstance(indexDefinition.Name);
                                    if (instance == null)
                                    {
                                        //probably deleted
                                        continue;
                                    }

                                    indexesToAdd.Add(new IndexToAdd
                                    {
                                        Name = indexDefinition.Name,
                                        Definition = indexDefinition,
                                        Priority = instance.Priority
                                    });
                                }

                                ReplicateIndexesMultiPut(destination, indexesToAdd);
                            }

                            Database.TransactionalStorage.Batch(actions =>
                            {
                                foreach (var indexTombstone in replicatedIndexTombstones)
                                {
                                    if (indexTombstone.Value != replicationDestinations.Count &&
                                        Database.IndexStorage.HasIndex(indexTombstone.Key) == false)
                                    {
                                        continue;
                                    }

                                    actions.Lists.Remove(Constants.RavenReplicationIndexesTombstones, indexTombstone.Key);
                                }
                            });
                        }
                        catch (Exception e)
                        {
                            Log.ErrorException("Failed to replicate indexes to " + destination, e);
                        }
                    }

                    return true;
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to replicate indexes", e);

                return false;
            }
            finally
            {
                Monitor.Exit(indexReplicationLock);
            }
        }

        public void Execute(string indexName)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(indexName);

            if (definition == null)
                return;

            if(definition.IsSideBySideIndex)
                return;

            var destinations = GetReplicationDestinations(x => x.SkipIndexReplication == false);

            var sideBySideIndexes = Database.Indexes.Definitions.Where(x => x.IsSideBySideIndex).ToDictionary(x => x.Name, x => x);

            IndexDefinition sideBySideIndexDefinition;
            if (sideBySideIndexes.TryGetValue("ReplacementOf/" + definition.Name, out sideBySideIndexDefinition))
            {
                foreach (var destination in destinations)
                {
                    ReplicateSingleSideBySideIndex(destination, definition, sideBySideIndexDefinition);
                }
            }
            else
            {
                foreach (var destination in destinations)
                {
                    var instance = Database.IndexStorage.GetIndexInstance(definition.Name);
                    if (instance == null)
                        continue;

                    ReplicateIndexesMultiPut(destination, new List<IndexToAdd>
                    {
                        new IndexToAdd
                        {
                            Name = definition.Name,
                            Definition = definition,
                            Priority = instance.Priority
                        }
                    });
                }
            }
        }

        private void OnIndexChange(DocumentDatabase documentDatabase, IndexChangeNotification notification)
        {
            var indexName = notification.Name;
            switch (notification.Type)
            {
                case IndexChangeTypes.IndexAdded:
                    if (notification.Version.HasValue == false)
                        return;

                    //if created index with the same name as deleted one, we should prevent its deletion replication
                    Database.TransactionalStorage.Batch(
                        accessor =>
                        {
                            var li = accessor.Lists.Read(Constants.RavenReplicationIndexesTombstones, indexName);
                            if (li == null)
                                return;

                            int version;
                            var versionStr = li.Data.Value<string>("IndexVersion");
                            if (int.TryParse(versionStr, out version))
                            {
                                if (version < notification.Version.Value)
                                {
                                    accessor.Lists.Remove(Constants.RavenReplicationIndexesTombstones, indexName);
                                }
                            }
                            else
                            {
                                Log.Error("Failed to parse index version of index {0}", indexName);
                            }					        
                        });
                    break;
                case IndexChangeTypes.IndexRemoved:
                    var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(indexName);
                    if (indexDefinition != null)
                    {
                        //the side by side index was deleted
                        IndexToAdd _;
                        sideBySideIndexesToReplicate.TryRemove(indexDefinition.IndexId, out _);
                    }
                    
                    //If we don't have any destination to replicate to (we are probably slave node)
                    //we shouldn't keep a tombstone since we are not going to remove it anytime
                    //and keeping it prevents us from getting that index created again.
                    if (GetReplicationDestinations().Count == 0)
                        return;

                    var metadata = new RavenJObject
                    {
                        {Constants.RavenIndexDeleteMarker, true},
                        {Constants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
                        {Constants.RavenReplicationVersion, ReplicationHiLo.NextId(Database)},
                        {"IndexVersion", notification.Version }
                    };
                    
                    Database.TransactionalStorage.Batch(accessor => accessor.Lists.Set(Constants.RavenReplicationIndexesTombstones, indexName, metadata, UuidType.Indexing));
                    break;
            }
        }

        private void ReplicateIndexesMultiPut(ReplicationStrategy destination, List<IndexToAdd> indexesToAdd)
        {
            var serializedIndexDefinitions = RavenJToken.FromObject(indexesToAdd.ToArray());
            var url = string.Format("{0}/indexes?{1}", destination.ConnectionStringOptions.Url, GetDebugInformation());

            var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethods.Put, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
            replicationRequest.Write(serializedIndexDefinitions);
            replicationRequest.ExecuteRequest();
        }

        private void ReplicateSideBySideIndexesMultiPut(ReplicationStrategy destination, List<IndexToAdd> indexes)
        {
            var sideBySideIndexes = new SideBySideIndexes
            {
                IndexesToAdd = indexes.ToArray()
            };

            var serializedIndexDefinitions = RavenJToken.FromObject(sideBySideIndexes);
            var url = $"{destination.ConnectionStringOptions.Url}/side-by-side-indexes?{GetDebugInformation()}";

            var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethods.Put, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
            replicationRequest.Write(serializedIndexDefinitions);
            replicationRequest.ExecuteRequest();
        }

        private void ReplicateSingleSideBySideIndex(ReplicationStrategy destination, IndexDefinition indexDefinition, IndexDefinition sideBySideIndexDefinition)
        {
            var url = string.Format("{0}/replication/side-by-side?{1}", destination.ConnectionStringOptions.Url, GetDebugInformation());
            IndexReplaceDocument indexReplaceDocument;

            try
            {
                indexReplaceDocument = Database.Documents.Get(Constants.IndexReplacePrefix + sideBySideIndexDefinition.Name, null).DataAsJson.JsonDeserialization<IndexReplaceDocument>();
            }
            catch (Exception e)
            {
                Log.Warn("Cannot get side-by-side index replacement document. Aborting operation. (this exception should not happen and the cause should be investigated)", e);
                return;
            }

            var sideBySideReplicationInfo = new SideBySideReplicationInfo
            {
                Index = indexDefinition,
                SideBySideIndex = sideBySideIndexDefinition,
                OriginDatabaseId = destination.CurrentDatabaseId,
                IndexReplaceDocument = indexReplaceDocument
            };

            var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethod.Post, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
            replicationRequest.Write(RavenJObject.FromObject(sideBySideReplicationInfo));
            replicationRequest.ExecuteRequest();
        }

        private void ReplicateIndexDeletionIfNeeded(List<JsonDocument> indexTombstones, ReplicationStrategy destination, Dictionary<string, int> replicatedIndexTombstones)
        {
            if (indexTombstones.Count == 0)
                return;

            foreach (var tombstone in indexTombstones)
            {
                try
                {
                    int value;
                    //In case the index was recreated under the same name we will increase the destination count for this tombstone 
                    //As if we sent the delete request but without actually sending the request, ending with a NOOP and deleting the index tombstone.
                    if (Database.IndexStorage.HasIndex(tombstone.Key)) 
                    {
                        replicatedIndexTombstones.TryGetValue(tombstone.Key, out value);
                        replicatedIndexTombstones[tombstone.Key] = value + 1;
                        continue;
                    }

                    var url = string.Format("{0}/indexes/{1}?{2}", destination.ConnectionStringOptions.Url, Uri.EscapeUriString(tombstone.Key), GetDebugInformation());
                    var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethods.Delete, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
                    replicationRequest.Write(RavenJObject.FromObject(EmptyRequestBody));
                    replicationRequest.ExecuteRequest();
                    Log.Info("Replicated index deletion (index name = {0})", tombstone.Key);

                    replicatedIndexTombstones.TryGetValue(tombstone.Key, out value);
                    replicatedIndexTombstones[tombstone.Key] = value + 1;
                }
                catch (Exception e)
                {
                    Replication.HandleRequestBufferingErrors(e, destination);

                    Log.ErrorException(string.Format("Failed to replicate index deletion (index name = {0})", tombstone.Key), e);
                }
            }
        }

        public void SendLastQueried()
        {
            if (Database.Disposed)
                return;

            try
            {
                using (CultureHelper.EnsureInvariantCulture())
                {
                    var relevantIndexLastQueries = new Dictionary<string, DateTime>();
                    var relevantIndexes = Database.Statistics.Indexes.Where(indexStats => indexStats.IsInvalidIndex == false && indexStats.Priority != IndexingPriority.Error && indexStats.Priority != IndexingPriority.Disabled && indexStats.LastQueryTimestamp.HasValue);

                    foreach (var relevantIndex in relevantIndexes)
                    {
                        relevantIndexLastQueries[relevantIndex.Name] = relevantIndex.LastQueryTimestamp.GetValueOrDefault();
                    }

                    if (relevantIndexLastQueries.Count == 0) return;

                    var destinations = GetReplicationDestinations(x => x.SkipIndexReplication == false);

                    foreach (var destination in destinations)
                    {
                        try
                        {
                            string url = destination.ConnectionStringOptions.Url + "/indexes/last-queried";

                            var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
                            replicationRequest.Write(RavenJObject.FromObject(relevantIndexLastQueries));
                            replicationRequest.ExecuteRequest();
                        }
                        catch (Exception e)
                        {
                            Replication.HandleRequestBufferingErrors(e, destination);

                            Log.WarnException("Could not update last query time of " + destination.ConnectionStringOptions.Url, e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to send last queried timestamp of indexes", e);
            }
        }

        public override void Dispose()
        {
            if (indexReplicationTimer != null)
                indexReplicationTimer.Dispose();

            if (lastQueriedTimer != null)
                lastQueriedTimer.Dispose();
        }
    }
}
