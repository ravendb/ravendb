using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;

using Rachis.Storage;

using Raven.Abstractions;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Responders;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection;
using Raven.Database.Actions;
using Raven.Database.Bundles.Replication.Plugins;
using Raven.Database.Bundles.Replication.Utils;
using Raven.Database.Config;
using Raven.Database.Queries;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
    public class ReplicationController : BundlesApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        public override string BundleName
        {
            get { return "replication"; }
        }

        private ReplicationTask replicationTask;
        public ReplicationTask ReplicationTask
        {
            get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
        }

        public IEnumerable<AbstractDocumentReplicationConflictResolver> DocsReplicationConflictResolvers
        {
            get
            {
                var exported = Database.Configuration.Container.GetExportedValues<AbstractDocumentReplicationConflictResolver>();

                var config = GetReplicationConfig();

                if (config == null || config.DocumentConflictResolution == StraightforwardConflictResolution.None)
                    return exported;

                var withConfiguredResolvers = exported.ToList();

                switch (config.DocumentConflictResolution)
                {
                    case StraightforwardConflictResolution.ResolveToLocal:
                        withConfiguredResolvers.Add(LocalDocumentReplicationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToRemote:
                        withConfiguredResolvers.Add(RemoteDocumentReplicationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToLatest:
                        withConfiguredResolvers.Add(LatestDocumentReplicationConflictResolver.Instance);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("config.DocumentConflictResolution");
                }

                return withConfiguredResolvers;
            }
        }

        [HttpGet]
        [RavenRoute("replication/explain/{*docId}")]
        [RavenRoute("databases/{databaseName}/replication/explain/{*docId}")]
        public HttpResponseMessage ExplainGet(string docId)
        {
            if (string.IsNullOrEmpty(docId)) 
                return GetMessageWithString("Document key is required.", HttpStatusCode.BadRequest);

            var destinationUrl = GetQueryStringValue("destinationUrl");
            if (string.IsNullOrEmpty(destinationUrl))
                return GetMessageWithString("Destination url is required.", HttpStatusCode.BadRequest);

            var databaseName = GetQueryStringValue("databaseName");
            if (string.IsNullOrEmpty(databaseName))
                return GetMessageWithString("Destination database name is required.", HttpStatusCode.BadRequest);

            var result = new ReplicationExplanationForDocument
            {
                Key = docId,
                Destination = new ReplicationExplanationForDocument.DestinationInformation
                {
                    Url = destinationUrl,
                    DatabaseName = databaseName
                }
            };

            var destinations = ReplicationTask.GetReplicationDestinations(x => string.Equals(x.Url, destinationUrl, StringComparison.OrdinalIgnoreCase) && string.Equals(x.Database, databaseName, StringComparison.OrdinalIgnoreCase));
            if (destinations == null || destinations.Length == 0)
            {
                result.Message = string.Format("Could not find replication destination for a given url ('{0}') and database ('{1}').", destinationUrl, databaseName);
                return GetMessageWithObject(result);
            }

            if (destinations.Length > 1)
            {
                result.Message = string.Format("There is more than one replication destination for a given url ('{0}') and database ('{1}').", destinationUrl, databaseName);
                return GetMessageWithObject(result);
            }

            var destination = destinations[0];
            var destinationsReplicationInformationForSource = ReplicationTask.GetLastReplicatedEtagFrom(destination);
            if (destinationsReplicationInformationForSource == null)
            {
                result.Message = "Could not connect to destination server.";
                return GetMessageWithObject(result);
            }

            var destinationId = destinationsReplicationInformationForSource.ServerInstanceId.ToString();
            result.Destination.ServerInstanceId = destinationId;
            result.Destination.LastDocumentEtag = destinationsReplicationInformationForSource.LastDocumentEtag;

            var document = Database.Documents.Get(docId, null);
            if (document == null)
            {
                result.Message = string.Format("Document with given key ('{0}') does not exist.", docId);
                return GetMessageWithObject(result);
            }

            result.Key = document.Key;
            result.Etag = document.Etag;

            string reason;
            if (destination.FilterDocuments(destinationId, document.Key, document.Metadata, out reason) == false)
            {
                result.Message = reason;
                return GetMessageWithObject(result);
            }

            reason = EtagUtil.IsGreaterThan(document.Etag, destinationsReplicationInformationForSource.LastDocumentEtag) ? "Document will be replicated." : "Document should have been replicated.";
            result.Message = reason;

            return GetMessageWithObject(result);
        }

        [HttpGet]
        [RavenRoute("replication/topology")]
        [RavenRoute("databases/{databaseName}/replication/topology")]
        public HttpResponseMessage TopologyGet()
        {
            if (Database == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            if (configurationDocument == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var mergedDocument = configurationDocument.MergedDocument;

            var isInCluster = ClusterManager.IsActive() && Database.IsClusterDatabase();
            var commitIndex = isInCluster ? ClusterManager.Engine.CommitIndex : -1;
            var term = isInCluster ? ClusterManager.Engine.PersistentState.CurrentTerm : -1;
            var currentTopology = isInCluster ? ClusterManager.Engine.CurrentTopology : null;
            var currentLeader = isInCluster ? ClusterManager.Engine.CurrentLeader:null;
            var isLeader = currentLeader != null && currentLeader == ClusterManager.Engine.Options.SelfConnection.Name;

            var configurationDocumentWithClusterInformation = new ReplicationDocumentWithClusterInformation
            {
                ClientConfiguration = mergedDocument.ClientConfiguration,
                Id = mergedDocument.Id,
                Source = mergedDocument.Source,
                ClusterCommitIndex = commitIndex,
                Term = term
            };

            if (isInCluster)
                configurationDocumentWithClusterInformation.ClusterInformation = new ClusterInformation(true, isLeader);

            foreach (var destination in mergedDocument.Destinations)
            {
                var destinationIsLeader = isInCluster && isLeader == false && currentTopology != null && currentLeader != null;
                if (destinationIsLeader)
                {
                    var destinationUrl = RaftHelper.GetNormalizedNodeUrl(destination.Url);
                    var node = currentTopology.AllVotingNodes.FirstOrDefault(x => x.Uri.AbsoluteUri.ToLowerInvariant() == destinationUrl);
                    if (node != null)
                        destinationIsLeader = node.Name == currentLeader;
                    else
                        destinationIsLeader = false;
        }

                configurationDocumentWithClusterInformation
                    .Destinations
                    .Add(ReplicationDestination.ReplicationDestinationWithClusterInformation.Create(destination, isInCluster, destinationIsLeader));
            }

            return GetMessageWithObject(configurationDocumentWithClusterInformation);
        }

        [Obsolete("Use RavenFS instead.")]
        public IEnumerable<AbstractAttachmentReplicationConflictResolver> AttachmentReplicationConflictResolvers
        {
            get
            {
                var exported = Database.Configuration.Container.GetExportedValues<AbstractAttachmentReplicationConflictResolver>();

                var config = GetReplicationConfig();

                if (config == null || config.AttachmentConflictResolution == StraightforwardConflictResolution.None)
                    return exported;

                var withConfiguredResolvers = exported.ToList();

                switch (config.AttachmentConflictResolution)
                {
                    case StraightforwardConflictResolution.ResolveToLocal:
                        withConfiguredResolvers.Add(LocalAttachmentReplicationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToRemote:
                        withConfiguredResolvers.Add(RemoteAttachmentReplicationConflictResolver.Instance);
                        break;
                    case StraightforwardConflictResolution.ResolveToLatest:
                        // ignore this resolver for attachments
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("config.AttachmentConflictResolution");
                }

                return withConfiguredResolvers;
            }
        }

        private const int ConflictBatchSize = 1024;

        [HttpGet]
        [RavenRoute("replication/forceConflictResolution")]
        [RavenRoute("databases/{databaseName}/replication/forceConflictResolution")]
        public HttpResponseMessage ForceConflictResolution()
        {
            long operationId;
            var cts = new CancellationTokenSource();

            var status = new ConflictResolveStatus
            {
                FailedConflictResolvingAttempts = 0
            };
            Task conflictResolvingTask = Task.Run(() =>
            {
                var resultsProcessed = 0;
                status.ConflictsResolved = 0;
                List<ConflictToResolve> conflicts = new List<ConflictToResolve>();
                var res = Database.Queries.Query(Constants.ConflictDocumentsIndex, new IndexQuery {Query = String.Empty, PageSize = int.MaxValue}, cts.Token);
                //res.Results is a List<> so res.Results.Count is O(1)
                status.TotalConflicts = res.Results.Count;
                res.Results.ForEach(conflict =>
                {
                    cts.Token.ThrowIfCancellationRequested();
                    AddSingleConflict(conflict, conflicts);
                    resultsProcessed++;
                    if (resultsProcessed % ConflictBatchSize == 0)
                    {
                        status.ConflictsResolved += HandleBatchOfConflicts(conflicts, status);
                    }
                });
                //Handle the remaining conflicts (last batch)
                status.ConflictsResolved += HandleBatchOfConflicts(conflicts, status);
            }, cts.Token);
            conflictResolvingTask.ContinueWith(_ =>
            {
                cts.Dispose();
            });
            Database.Tasks.AddTask(conflictResolvingTask, new TaskBasedOperationState(conflictResolvingTask, () => RavenJObject.FromObject(status)), new TaskActions.PendingTaskDescription
            {
                StartTime = DateTime.UtcNow,TaskType = TaskActions.PendingTaskType.ResolveConflicts
            }, out operationId, cts);

            return GetMessageWithObject(new
            {
                OperationId = operationId
            }, HttpStatusCode.Accepted);
        }

        private int HandleBatchOfConflicts(List<ConflictToResolve> conflicts, ConflictResolveStatus status)
        {
            int i = 0;
            using (Database.DocumentLock.Lock())
            {
                Database.TransactionalStorage.Batch(actions =>
                {                    
                    conflicts.ForEach(c => { HandleSingleConflictResolving(actions, c, status, ref i); });
                });
            }
            conflicts.Clear();
            return i;
        }

        private void HandleSingleConflictResolving(IStorageActionsAccessor actions, ConflictToResolve c, ConflictResolveStatus status,ref int i)
        {
            try
            {
                var replicationBehavior = new DocumentReplicationBehavior
                {
                    Actions = actions,
                    Database = Database,
                    ReplicationConflictResolvers = DocsReplicationConflictResolvers,
                    Src = "DontCare"
                };
                replicationBehavior.ResolveConflict(c.Id, c.Metadata, c.Document,c.ExistingDocument); 
            }
            catch (Exception e)
            {
                status.FailedConflictResolvingAttempts++;
                log.InfoException($"Failed to resolve conflict for document key: {c}", e);
            }
            finally
            {
                i++;
            }
        }

        private void AddSingleConflict(RavenJObject conflict , List<ConflictToResolve> conflicts)
        {
            var conflictsJArray = conflict.Value<RavenJArray>("Conflicts");
            JsonDocument conflict1 = null;
            JsonDocument conflict2 = null;
            Database.TransactionalStorage.Batch(actions =>
            {
                conflict1 = actions.Documents.DocumentByKey(conflictsJArray[0].Value<string>());
                conflict2 = actions.Documents.DocumentByKey(conflictsJArray[1].Value<string>());
            });
            
            JsonDocument remote;
            JsonDocument local;
            if (!conflict1.Metadata.Value<string>(Constants.RavenReplicationSource).Equals(Database.TransactionalStorage.Id.ToString()))
            {
                remote = conflict1;
                local = conflict2;
            }
            else
            {
                remote = conflict2;
                local = conflict1;
            }
            var id = conflict.Value<RavenJObject>("@metadata").Value<string>("@id");
            conflicts.Add(new ConflictToResolve {Id=id,Document = remote.DataAsJson,Metadata = remote.Metadata, ExistingDocument = local});
        }

        private class ConflictToResolve
        {
            public string Id { get; set; }
            public RavenJObject Document { get; set; }
            public RavenJObject Metadata { get; set; }
            public JsonDocument ExistingDocument { get; set; }
        }

        private class ConflictResolveStatus
        {
            public long FailedConflictResolvingAttempts { get; set; }
            public long ConflictsResolved { get; set; }
            public long TotalConflicts { get; set; }
        }

        [HttpPost]
        [RavenRoute("replication/replicateDocs")]
        [RavenRoute("databases/{databaseName}/replication/replicateDocs")]
        public async Task<HttpResponseMessage> DocReplicatePost()
        {
            const int BatchSize = 512;
            var topologyId = Request.Headers.GetFirstValue("Topology-Id");
            if (!string.IsNullOrEmpty(topologyId) && topologyId != Database.ClusterManager?.Value?.Engine.CurrentTopology.TopologyId.ToString())
            {
                return GetMessageWithString("Refusing to accept data outside of my topology",HttpStatusCode.Forbidden);
            }
            var src = GetQueryStringValue("from");
            var collections = GetQueryStringValue("collections");
            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            while (src.EndsWith("/"))
                src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven

            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var array = await ReadJsonArrayAsync().ConfigureAwait(false);
            try
            {
                if (ReplicationTask != null)
                {
                    //indicates to the replication task that this thread is going to insert documents.
                    ReplicationTask.IsThreadProcessingReplication.Value = true;
                    ReplicationTask.HandleHeartbeat(src);
                }

                using (Database.DisableAllTriggersForCurrentThread())
                {
                    var conflictResolvers = DocsReplicationConflictResolvers;

                    string lastEtag = Etag.Empty.ToString();

                    var docIndex = 0;
                    var retries = 0;
                    while (retries < 3 && docIndex < array.Length)
                    {
                        var lastIndex = docIndex;
                        using (Database.DocumentLock.Lock())
                        {
                            Database.TransactionalStorage.Batch(actions =>
                            {
                                for (var j = 0; j < BatchSize && docIndex < array.Length; j++, docIndex++)
                                {
                                    var document = (RavenJObject)array[docIndex];
                                    var metadata = document.Value<RavenJObject>("@metadata");
                                    if (metadata[Constants.RavenReplicationSource] == null)
                                    {
                                    // not sure why, old document from when the user didn't have replication
                                    // that we suddenly decided to replicate, choose the source for that
                                    metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
                                    }

                                    lastEtag = metadata.Value<string>("@etag");
                                    var id = metadata.Value<string>("@id");
                                    document.Remove("@metadata");
                                    ReplicateDocument(actions, id, metadata, document, src, conflictResolvers);
                                }

                                SaveReplicationSource(src, lastEtag, array.Length, collections);
                                retries = lastIndex == docIndex ? retries : 0;
                            });
                        }

                        if (lastIndex == docIndex)
                        {

                            if (retries == 3)
                            {
                                Log.Warn("Replication processing did not end up replicating any documents for 3 times in a row, stopping operation", retries);
                            }
                            else
                            {
                                Log.Warn("Replication processing did not end up replicating any documents, due to possible storage error, retry number: {0}", retries);
                            }
                            retries++;
                        }
                    }
                }                
            }
            finally
            {
                //indicates that this thread is no longer sending documents.
                if (ReplicationTask != null)
                    ReplicationTask.IsThreadProcessingReplication.Value = false;
            }
            return GetEmptyMessage();
        }

        private void SaveReplicationSource(string src, string lastEtag, int batchSize, string collections = null)
        {
            Guid remoteServerInstanceId = Guid.Parse(GetQueryStringValue("dbid"));

            var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId;
            if (!string.IsNullOrEmpty(collections))
                replicationDocKey += ("/" + collections);

            var replicationDocument = Database.Documents.Get(replicationDocKey, null);
            var lastAttachmentId = Etag.Empty;
            if (replicationDocument != null)
            {
                lastAttachmentId = replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().LastAttachmentEtag;
            }

            Database
                .Documents
                .Put(
                    replicationDocKey,
                    null,
                    RavenJObject.FromObject(
                        new SourceReplicationInformation
                        {
                            Source = src,
                            LastDocumentEtag = Etag.Parse(lastEtag),
                            LastAttachmentEtag = lastAttachmentId,
                            ServerInstanceId = remoteServerInstanceId,
                            SourceCollections = collections,
                            LastModified = SystemTime.UtcNow,
                            LastBatchSize = batchSize
                        }),
                    new RavenJObject(),
                    null);
        }

        [HttpPost]
        [RavenRoute("replication/replicateAttachments")]
        [RavenRoute("databases/{databaseName}/replication/replicateAttachments")]
        [Obsolete("Use RavenFS instead.")]
        public async Task<HttpResponseMessage> AttachmentReplicatePost()
        {
            var topologyId = Request.Headers.GetFirstValue("Topology-Id");
            if (topologyId != null && topologyId != Database.ClusterManager?.Value?.Engine.CurrentTopology.TopologyId.ToString())
            {
                return GetMessageWithString("Refusing to accept data outside of my topology", HttpStatusCode.Forbidden);
            }
            var src = GetQueryStringValue("from");
            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            while (src.EndsWith("/"))
                src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            try
            {
                if (ReplicationTask != null)
                {
                    //indicates to the replication task that this thread is going to insert attachments.
                    ReplicationTask.IsThreadProcessingReplication.Value = true;
                }
                var array = await ReadBsonArrayAsync().ConfigureAwait(false);
                using (Database.DisableAllTriggersForCurrentThread())
                {
                    var conflictResolvers = AttachmentReplicationConflictResolvers;

                    Database.TransactionalStorage.Batch(actions =>
                    {
                        Etag lastEtag = Etag.Empty;
                        foreach (RavenJObject attachment in array)
                        {
                            var metadata = attachment.Value<RavenJObject>("@metadata");
                            if (metadata[Constants.RavenReplicationSource] == null)
                            {
                            // not sure why, old attachment from when the user didn't have replication
                            // that we suddenly decided to replicate, choose the source for that
                            metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
                            }

                            lastEtag = Etag.Parse(attachment.Value<byte[]>("@etag"));
                            var id = attachment.Value<string>("@id");

                            ReplicateAttachment(actions, id, metadata, attachment.Value<byte[]>("data"), src, conflictResolvers);
                        }

                        Guid remoteServerInstanceId = Guid.Parse(GetQueryStringValue("dbid"));

                        var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId;
                        var replicationDocument = Database.Documents.Get(replicationDocKey, null);
                        Etag lastDocId = null;
                        if (replicationDocument != null)
                        {
                            lastDocId =
                                replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
                                    LastDocumentEtag;
                        }

                        Database.Documents.Put(replicationDocKey, null,
                                     RavenJObject.FromObject(new SourceReplicationInformation
                                     {
                                         Source = src,
                                         LastDocumentEtag = lastDocId,
                                         LastAttachmentEtag = lastEtag,
                                         ServerInstanceId = remoteServerInstanceId,
                                         LastModified = SystemTime.UtcNow
                                     }),
                                     new RavenJObject(), null);
                    });
                }
            }
            finally
            {
                //indicates that this thread is no longer sending attachments.
                if (ReplicationTask != null)
                    ReplicationTask.IsThreadProcessingReplication.Value = false;
            }            
            return GetEmptyMessage();
        }

        [HttpGet]
        [HttpPost]
        [RavenRoute("replication/info")]
        [RavenRoute("databases/{databaseName}/replication/info")]
        public HttpResponseMessage ReplicationInfoGet()
        {
            var replicationStatistics = ReplicationUtils.GetReplicationInformation(Database);
            return GetMessageWithObject(replicationStatistics);
        }

        [HttpGet]
        [RavenRoute("replication/lastEtag")]
        [RavenRoute("databases/{databaseName}/replication/lastEtag")]
        public HttpResponseMessage ReplicationLastEtagGet()
        {
            string src;
            string dbid;
            string collections;
            var result = GetValuesForLastEtag(out src, out dbid, out collections);
            if (result != null)
                return result;

            using (Database.DisableAllTriggersForCurrentThread())
            {
                JsonDocument document = null;
                SourceReplicationInformationWithBatchInformation sourceReplicationInformation = null;

                var localServerInstanceId = Database.TransactionalStorage.Id; // this is my id, sent to the remote server

                if (string.IsNullOrEmpty(dbid))
                {
                    // backward compatibility for replication behavior
                    int nextStart = 0;
                    var replicationSources = Database.Documents.GetDocumentsWithIdStartingWith(Constants.RavenReplicationSourcesBasePath, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);
                    foreach (RavenJObject replicationSource in replicationSources)
                    {
                        sourceReplicationInformation = replicationSource.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();
                        if (string.Equals(sourceReplicationInformation.Source, src, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        document = replicationSource.ToJsonDocument();
                        break;
                    }
                }
                else
                {
                    var remoteServerInstanceId = Guid.Parse(dbid);

                    var docKey = Constants.RavenReplicationSourcesBasePath + "/" + remoteServerInstanceId;
                    if (!String.IsNullOrEmpty(collections))
                        docKey += ("/" + collections);

                    document = Database.Documents.Get(docKey, null);
                    if (document == null)
                    {
                        // migrate
                        document = Database.Documents.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);
                        if (document != null)
                        {
                            sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();
                            Database.Documents.Put(docKey, Etag.Empty, document.DataAsJson, document.Metadata, null);
                            Database.Documents.Delete(Constants.RavenReplicationSourcesBasePath + "/" + src, document.Etag, null);

                            if (remoteServerInstanceId != sourceReplicationInformation.ServerInstanceId) 
                                document = null;
                        }
                    }
                }

                if (document == null)
                {
                    sourceReplicationInformation = new SourceReplicationInformationWithBatchInformation
                    {
                        Source = src,
                        ServerInstanceId = localServerInstanceId,
                        LastModified = SystemTime.UtcNow
                    };
                }
                else
                {
                    if (sourceReplicationInformation == null)
                        sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformationWithBatchInformation>();

                    if (string.Equals(sourceReplicationInformation.Source, src, StringComparison.OrdinalIgnoreCase) == false 
                        && sourceReplicationInformation.LastModified.HasValue 
                        && (SystemTime.UtcNow - sourceReplicationInformation.LastModified.Value).TotalMinutes < 10)
                    {
                        log.Info(string.Format("Replication source mismatch. Stored: {0}. Remote: {1}.", sourceReplicationInformation.Source, src));

                        sourceReplicationInformation.LastAttachmentEtag = Etag.InvalidEtag;
                        sourceReplicationInformation.LastDocumentEtag = Etag.InvalidEtag;
                    }

                    sourceReplicationInformation.ServerInstanceId = localServerInstanceId;
                }

                var maxNumberOfItemsToReceiveInSingleBatch = Database.Configuration.Replication.MaxNumberOfItemsToReceiveInSingleBatch;
                var availableMemory = MemoryStatistics.AvailableMemoryInMb;
                var lowMemory = availableMemory < 0.2 * MemoryStatistics.TotalPhysicalMemory && availableMemory < Database.Configuration.AvailableMemoryForRaisingBatchSizeLimit * 2;
                if (lowMemory)
                {
                    int size;
                    var lastBatchSize = sourceReplicationInformation.LastBatchSize;
                    if (lastBatchSize.HasValue && maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                        size = Math.Min(lastBatchSize.Value, maxNumberOfItemsToReceiveInSingleBatch.Value);
                    else if (lastBatchSize.HasValue)
                        size = lastBatchSize.Value;
                    else if (maxNumberOfItemsToReceiveInSingleBatch.HasValue)
                        size = maxNumberOfItemsToReceiveInSingleBatch.Value;
                    else
                        size = 128;

                    sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch =
                        Math.Max(size / 2, 64);
                }
                else
                {
                    sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch = Database.Configuration.Replication.MaxNumberOfItemsToReceiveInSingleBatch;
                }

                var currentEtag = GetQueryStringValue("currentEtag");
                if (Log.IsDebugEnabled)
                    Log.Debug(() => string.Format("Got replication last etag request from {0}: [Local: {1} Remote: {2}]. LowMemory: {3}. MaxNumberOfItemsToReceiveInSingleBatch: {4}.", src, sourceReplicationInformation.LastDocumentEtag, currentEtag, lowMemory, sourceReplicationInformation.MaxNumberOfItemsToReceiveInSingleBatch));
                return GetMessageWithObject(sourceReplicationInformation);
            }
        }

        [HttpPut]
        [RavenRoute("replication/lastEtag")]
        [RavenRoute("databases/{databaseName}/replication/lastEtag")]
        public HttpResponseMessage ReplicationLastEtagPut()
        {
            string src;
            string dbid;
            string collections;
            var result = GetValuesForLastEtag(out src, out dbid, out collections);
            if (result != null)
                return result;

            using (Database.DisableAllTriggersForCurrentThread())
            {
                var key = Constants.RavenReplicationSourcesBasePath + "/" + dbid;
                if (!String.IsNullOrEmpty(collections))
                    key += ("/" + collections);

                var document = Database.Documents.Get(key, null);

                SourceReplicationInformation sourceReplicationInformation;

                Etag docEtag = null, attachmentEtag = null;
                try
                {
                    docEtag = Etag.Parse(GetQueryStringValue("docEtag"));
                }
                catch
                {
                }

                try
                {
                    attachmentEtag = Etag.Parse(GetQueryStringValue("attachmentEtag"));
                }
                catch
                {
                }

                Guid serverInstanceId = Guid.Parse(dbid);

                if (document == null)
                {
                    sourceReplicationInformation = new SourceReplicationInformation()
                    {
                        ServerInstanceId = serverInstanceId,
                        LastAttachmentEtag = attachmentEtag ?? Etag.Empty,
                        LastDocumentEtag = docEtag ?? Etag.Empty,
                        Source = src,
                        LastModified = SystemTime.UtcNow
                    };
                }
                else
                {
                    sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                    sourceReplicationInformation.ServerInstanceId = serverInstanceId;
                    sourceReplicationInformation.LastDocumentEtag = docEtag ?? sourceReplicationInformation.LastDocumentEtag;
                    sourceReplicationInformation.LastAttachmentEtag = attachmentEtag ?? sourceReplicationInformation.LastAttachmentEtag;
                    sourceReplicationInformation.LastModified = SystemTime.UtcNow;
                }

                var etag = document == null ? Etag.Empty : document.Etag;
                var metadata = document == null ? new RavenJObject() : document.Metadata;

                var newDoc = RavenJObject.FromObject(sourceReplicationInformation);
                if (log.IsDebugEnabled)
                    log.Debug("Updating replication last etags from {0}: [doc: {1} attachment: {2}]", src,
                                  sourceReplicationInformation.LastDocumentEtag,
                                  sourceReplicationInformation.LastAttachmentEtag);

                Database.Documents.Put(key, etag, newDoc, metadata, null);
            }

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("replication/heartbeat")]
        [RavenRoute("databases/{databaseName}/replication/heartbeat")]
        public HttpResponseMessage HeartbeatPost()
        {
            var src = GetQueryStringValue("from");

            var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            if (replicationTask == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "Cannot find replication task setup in the database"
                }, HttpStatusCode.NotFound);

            }

            replicationTask.HandleHeartbeat(src);

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("replication/side-by-side/")]
        [RavenRoute("databases/{databaseName}/replication/side-by-side/")]
        public HttpResponseMessage ReplicateSideBySideIndex([FromBody] SideBySideReplicationInfo sideBySideReplicationInfo)
        {
            var index = Database.Indexes.GetIndexDefinition(sideBySideReplicationInfo.Index.Name);
            var sideBySideIndex = Database.Indexes.GetIndexDefinition(sideBySideReplicationInfo.SideBySideIndex.Name);

            //handle special cases first
            if (index == null)
            {
                //if there is no main index we would recreate it with side-by-side index definition,
                //but if something happened and the old index was deleted and the side-by-side index was not deleted,
                //then it is not needed anymore and should be deleted
                if (sideBySideIndex != null)
                {
                    using (Database.DisableAllTriggersForCurrentThread())//prevent this from being replicated as this change is internal and should not be replicated
                    using (Database.DocumentLock.Lock()) //prevent race condition -> simultaneously with replication to this node, 
                                                         //a client creates side-by-side index
                    {
                        Database.Indexes.DeleteIndex(sideBySideIndex.Name);
                        var id = Constants.IndexReplacePrefix + sideBySideReplicationInfo.SideBySideIndex.Name;
                        Database.Documents.Delete(id, null, null);
                    }
                }

                return InternalPutIndex(sideBySideReplicationInfo.Index.Name,
                    sideBySideReplicationInfo.SideBySideIndex,
                    string.Format("Index with the name {0} wasn't found, so we created it with side-by-side index definition. (Perhaps it was deleted?)", sideBySideReplicationInfo.Index.Name));
            }

            if (index.Equals(sideBySideReplicationInfo.SideBySideIndex, false))
                return GetMessageWithObject(new
                {
                    Message = "It appears that side-by-side index already replaced the old index. Nothing to do..."
                }, HttpStatusCode.NotModified);

            //if both side-by-side index and the original index are identical, nothing to do here
            var areIndexesEqual = index.Equals(sideBySideReplicationInfo.Index, false);
            var areSideBySideIndexesEqual = (sideBySideIndex != null) && sideBySideIndex.Equals(sideBySideReplicationInfo.SideBySideIndex, false);

            if (areIndexesEqual && areSideBySideIndexesEqual)
                return GetMessageWithObject(new
                {
                    Message = "It appears that side-by-side index and the old index are the same. Nothing to do..."
                }, HttpStatusCode.NotModified);

            if (areIndexesEqual == false && areSideBySideIndexesEqual)
                return InternalPutIndex(sideBySideReplicationInfo.Index, "Side-by-side indexes were equal, updated the old index.");

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse -> for better readability
            if (areIndexesEqual && areSideBySideIndexesEqual == false)
            {
                var internalPutIndex = InternalPutIndex(sideBySideReplicationInfo.SideBySideIndex, "Indexes to be replaced were equal, updated the side-by-side index.");
                if (internalPutIndex.IsSuccessStatusCode)
                PutSideBySideIndexDocument(sideBySideReplicationInfo);
                return internalPutIndex;
            }

            var updateIndexResult = InternalPutIndex(sideBySideReplicationInfo.Index, "Side-by-side indexes were equal, updated the old index.");
            var updateSideBySideIndexResult = InternalPutIndex(sideBySideReplicationInfo.SideBySideIndex, "Indexes to be replaced were equal, updated the side-by-side index.");
            if (updateSideBySideIndexResult.IsSuccessStatusCode)
                PutSideBySideIndexDocument(sideBySideReplicationInfo);

            if (updateIndexResult.IsSuccessStatusCode && updateSideBySideIndexResult.IsSuccessStatusCode)
                return GetMessageWithObject(new
                {
                    Indexes = new[] { sideBySideReplicationInfo.Index.Name, sideBySideReplicationInfo.SideBySideIndex.Name },
                    Message = "Both index and side-by-side index were different, so we updated them both"
                }, HttpStatusCode.Created);

            return updateIndexResult.IsSuccessStatusCode == false ?
                updateIndexResult : updateSideBySideIndexResult;
        }

        private void PutSideBySideIndexDocument(SideBySideReplicationInfo sideBySideReplicationInfo)
        {
            using (Database.DocumentLock.Lock())
            {
                var id = Constants.IndexReplacePrefix + sideBySideReplicationInfo.SideBySideIndex.Name;
                var indexReplaceDocument = sideBySideReplicationInfo.IndexReplaceDocument;

                if (indexReplaceDocument.MinimumEtagBeforeReplace != null) //TODO : verify that this is OK -> not sure
                    indexReplaceDocument.MinimumEtagBeforeReplace = EtagUtil.Increment(Database.Statistics.LastDocEtag, 1);
                Database.TransactionalStorage.Batch(accessor => accessor.Documents.AddDocument(id, null, RavenJObject.FromObject(indexReplaceDocument), new RavenJObject()));
            }
        }

        private HttpResponseMessage InternalPutIndex(IndexDefinition indexToUpdate, string message)
        {
            return InternalPutIndex(indexToUpdate.Name, indexToUpdate, message);
        }

        private HttpResponseMessage InternalPutIndex(string indexName, IndexDefinition indexToUpdate, string message)
        {
            try
            {
                Database.Indexes.PutIndex(indexName, indexToUpdate);
                return GetMessageWithObject(new
                {
                    Index = indexToUpdate.Name,
                    Message = message
                }, HttpStatusCode.Created);
            }
            catch (Exception ex)
            {
                var compilationException = ex as IndexCompilationException;

                return GetMessageWithObject(new
                {
                    ex.Message,
                    IndexDefinitionProperty = compilationException != null ? compilationException.IndexDefinitionProperty : "",
                    ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
                    Error = ex.ToString()
                }, HttpStatusCode.BadRequest);
            }
        }
        
        [HttpPost]
        [RavenRoute("replication/replicate-indexes")]
        [RavenRoute("databases/{databaseName}/replication/replicate-indexes")]
        public HttpResponseMessage IndexReplicate([FromBody] ReplicationDestination replicationDestination)
        {
            var op = GetQueryStringValue("op");
            var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();

            if (replicationTask == null)
                return GetMessageWithString("Could not find replication task. Something is wrong here, check logs for more details", HttpStatusCode.BadRequest);

            if (string.Equals(op, "replicate-all-to-destination", StringComparison.InvariantCultureIgnoreCase))
            {
                replicationTask.IndexReplication.Execute(dest => dest.IsEqualTo(replicationDestination) && dest.SkipIndexReplication == false);

                return GetEmptyMessage();
            }

            var indexName = GetQueryStringValue("indexName");

            if (string.IsNullOrEmpty(indexName) == false)
            {
                replicationTask.IndexReplication.Execute(indexName);
            return GetEmptyMessage();
        }

            replicationTask.IndexReplication.Execute();
            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("replication/replicate-transformers")]
        [RavenRoute("databases/{databaseName}/replication/replicate-transformers")]
        public HttpResponseMessage TransformersReplicate([FromBody] ReplicationDestination replicationDestination)
        {
            var op = GetQueryStringValue("op");
            var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();

            if (replicationTask == null)
                return GetMessageWithString("Could not find replication task. Something is wrong here, check logs for more details", HttpStatusCode.BadRequest);

            if (string.Equals(op, "replicate-all-to-destination", StringComparison.InvariantCultureIgnoreCase))
            {
                replicationTask.TransformerReplication.Execute(dest => dest.IsEqualTo(replicationDestination) && dest.SkipIndexReplication == false);
                return GetEmptyMessage();
            }

            var transformerName = GetQueryStringValue("transformerName");

            if (string.IsNullOrEmpty(transformerName) == false)
            {
                replicationTask.TransformerReplication.Execute(transformerName);
            return GetEmptyMessage();
        }

            replicationTask.TransformerReplication.Execute();
            return GetEmptyMessage();
        }

        private HttpResponseMessage GetValuesForLastEtag(out string src, out string dbid, out string collections)
        {
            src = GetQueryStringValue("from");
            dbid = GetQueryStringValue("dbid");
            collections = GetQueryStringValue("collections");

            if (dbid == Database.TransactionalStorage.Id.ToString())
                throw new InvalidOperationException("Both source and target databases have database id = " + dbid +
                                                    "\r\nDatabase cannot replicate to itself.");

            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            while (src.EndsWith("/"))
                src = src.Substring(0, src.Length - 1); // remove last /, because that has special meaning for Raven
            if (string.IsNullOrEmpty(src))
                return GetEmptyMessage(HttpStatusCode.BadRequest);
            return null;
        }

        private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src, IEnumerable<AbstractDocumentReplicationConflictResolver> conflictResolvers)
        {
            new DocumentReplicationBehavior
            {
                Actions = actions,
                Database = Database,
                ReplicationConflictResolvers = conflictResolvers,
                Src = src
            }.Replicate(id, metadata, document);
        }

        [Obsolete("Use RavenFS instead.")]
        private void ReplicateAttachment(IStorageActionsAccessor actions, string id, RavenJObject metadata, byte[] data, string src, IEnumerable<AbstractAttachmentReplicationConflictResolver> conflictResolvers)
        {
            new AttachmentReplicationBehavior
            {
                Actions = actions,
                Database = Database,
                ReplicationConflictResolvers = conflictResolvers,
                Src = src
            }.Replicate(id, metadata, data);
        }

        private ReplicationConfig GetReplicationConfig()
        {
            var configDoc = Database.Documents.Get(Constants.RavenReplicationConfig, null);

            if (configDoc == null)
                return null;

            ReplicationConfig config;
            try
            {
                config = configDoc.DataAsJson.JsonDeserialization<ReplicationConfig>();
                return config;
            }
            catch (Exception e)
            {
                Log.Warn("Could not deserialize a replication config", e);
                return null;
            }
        }

        private class ReplicationExplanationForDocument
        {
            public string Key { get; set; }

            public Etag Etag { get; set; }

            public DestinationInformation Destination { get; set; }

            public string Message { get; set; }

            public class DestinationInformation
            {
                public string Url { get; set; }

                public string DatabaseName { get; set; }

                public string ServerInstanceId { get; set; }

                public Etag LastDocumentEtag { get; set; }
            }
        }
    }
}
