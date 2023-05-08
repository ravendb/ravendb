using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIncomingReplicationHandler : AbstractIncomingReplicationHandler<TransactionContextPool, TransactionOperationContext>
    {
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private readonly Dictionary<int, ShardedOutgoingReplicationHandler> _handlers;
        private string _lastAcceptedChangeVector;
        private long _lastAcceptedEtag;

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag)
            : base(parent, tcpConnectionOptions, buffer, replicatedLastEtag)
        {
            _parent = parent;
            _attachmentStreamsTempFile = GetTempFile();
            _handlers = new Dictionary<int, ShardedOutgoingReplicationHandler>(_parent.Context.ShardCount);

            foreach (var shardNumber in _parent.Context.ShardsTopology.Keys)
            {
                var node = new ShardReplicationNode { ShardNumber = shardNumber, Database = ShardHelper.ToShardName(_parent.DatabaseName, shardNumber) };
                var info = GetConnectionInfo(node);

                _handlers[shardNumber] = new ShardedOutgoingReplicationHandler(parent, node, info, replicatedLastEtag.SourceDatabaseId);
                _handlers[shardNumber].Start();
            }
        }

        public StreamsTempFile GetTempFile()
        {
            var name = $"attachment.{Guid.NewGuid():N}.shardedReplication";
            var tempPath = _parent.Context.ServerStore._env.Options.DataPager.Options.TempPath.Combine(name);
            return new StreamsTempFile(tempPath.FullPath, _parent.Context.Encrypted);
        }

        private TcpConnectionInfo GetConnectionInfo(ShardReplicationNode node)
        {
            var shardExecutor = _parent.Context.ShardExecutor;
            using (_parent.Context.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var cmd = new GetTcpInfoCommand("sharded-replication", node.Database);
                RequestExecutor requestExecutor = null;
                try
                {
                    requestExecutor = shardExecutor.GetRequestExecutorAt(node.ShardNumber);
                    requestExecutor.Execute(cmd, ctx);
                }
                finally
                {
                    // we want to set node Url even if we fail to connect to destination, so they can be used in replication stats
                    node.Url = requestExecutor?.Url;
                }

                return cmd.Result;
            }
        }

        protected override void EnsureNotDeleted() => _parent.EnsureNotDeleted(_parent.Server.NodeTag);

        protected override void InvokeOnFailed(Exception exception) => _parent.InvokeOnFailed(this, exception);

        protected override void HandleHeartbeatMessage(TransactionOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject)
        {
            blittableJsonReaderObject.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector);

            using (var replicationBatches = new ReplicationBatches(this))
            {
                var batches = replicationBatches.Batches;
                var tasks = new Task[_parent.Context.ShardCount];
                int i = 0;

                foreach (var (shardNumber, batch) in batches)
                {
                    batch.LastAcceptedChangeVector = changeVector ?? _lastAcceptedChangeVector;
                    batch.LastSentEtagFromSource = _lastAcceptedEtag;
                    tasks[i++] = _handlers[shardNumber].SendBatch(batch);
                }

                Task.WaitAll(tasks);

                var cvs = new List<string>();
                foreach (ReplicationBatch replicationBatch in batches.Values)
                {
                    cvs.Add(replicationBatch.LastAcceptedChangeVector);
                }

                _lastAcceptedChangeVector = ChangeVectorUtils.MergeVectorsDown(cvs);
            }
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(TransactionOperationContext context, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = base.GetHeartbeatStatusMessage(context, lastDocumentEtag, handledMessageType);
            heartbeat[nameof(ReplicationMessageReply.DatabaseChangeVector)] = _lastAcceptedChangeVector;
            return heartbeat;
        }

        internal async Task<(string MergedChangeVector, long LastEtag)> GetInitialHandshakeResponseFromShards()
        {
            // this is an optimization for replication failOvers.
            // in case of failOver, send back to the source the last change vector & Etag received on shards
            // so the replication won't start from scratch

            var lastAcceptedEtag = long.MaxValue;
            var handlersChangeVector = new List<string>();
            foreach (var handler in _handlers.Values)
            {
                var (acceptedChangeVector, acceptedEtag) = await handler.GetFirstChangeVectorFromShardAsync();
                handlersChangeVector.Add(acceptedChangeVector);
                lastAcceptedEtag = Math.Min(acceptedEtag, lastAcceptedEtag);
            }
            var mergedChangeVector = ChangeVectorUtils.MergeVectorsDown(handlersChangeVector);
            return (mergedChangeVector, lastAcceptedEtag);
        }

        protected override void InvokeOnDocumentsReceived()
        {
        }

        protected override void InvokeOnAttachmentStreamsReceived(int attachmentStreamCount)
        {
        }

        protected override async Task HandleBatchAsync(TransactionOperationContext context, DataForReplicationCommand dataForReplicationCommand, long lastEtag)
        {
            using (var replicationBatches = PrepareReplicationDataForShards(context, dataForReplicationCommand))
            {
                var batches = replicationBatches.Batches;
                var tasks = new Task[batches.Count];
                int i = 0;
                foreach (var shardToBatch in batches)
                {
                    shardToBatch.Value.LastSentEtagFromSource = _lastDocumentEtag;
                    tasks[i] = _handlers[shardToBatch.Key].SendBatch(shardToBatch.Value);
                    i++;
                }

                await Task.WhenAll(tasks);

                string missingAttachmentMessage = null;
                foreach (ShardedOutgoingReplicationHandler handler in _handlers.Values)
                {
                    missingAttachmentMessage ??= handler.MissingAttachmentMessage;
                    handler.MissingAttachmentMessage = null;
                }

                if (missingAttachmentMessage != null)
                    throw new MissingAttachmentException(missingAttachmentMessage);

                var cvs = new List<string>();
                var minEtag = long.MaxValue;
                foreach (ReplicationBatch replicationBatch in batches.Values)
                {
                    // ignore change vectors from heartbeat
                    if (replicationBatch.Items.Count > 0)
                        cvs.Add(replicationBatch.LastAcceptedChangeVector);

                    minEtag = Math.Min(minEtag, replicationBatch.LastEtagAccepted);
                }

                _lastAcceptedChangeVector = ChangeVectorUtils.MergeVectorsDown(cvs);
                _lastAcceptedEtag = minEtag;
            }
        }

        private ReplicationBatches PrepareReplicationDataForShards(TransactionOperationContext context, DataForReplicationCommand dataForReplicationCommand)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization possibility: instead of iterating over materialized batch, we can do it while reading from the stream");
            var replicationBatches = new ReplicationBatches(this);
            var batches = replicationBatches.Batches;

            foreach (var item in dataForReplicationCommand.ReplicatedItems)
            {
                if (item is RevisionTombstoneReplicationItem revisionTombstoneItem)
                {
                    if (SupportedFeatures.Replication.RevisionTombstonesWithId)
                    {
                        var docId = _documentInfoHelper.GetDocumentId(revisionTombstoneItem);
                        if (docId != null)
                        {
                            batches[_parent.Context.GetShardNumberFor(context, docId)].Items.Add(item);
                            continue;
                        }
                    }

                    foreach (var batch in batches.Values)
                        batch.Items.Add(revisionTombstoneItem);

                    continue;
                }

                int shardNumber = GetShardNumberForReplicationItem(context, item);
                batches[shardNumber].Items.Add(item);
                if (item is AttachmentReplicationItem attachmentReplicationItem && attachmentReplicationItem.Stream == null)
                {
                    batches[shardNumber].AttachmentStreams ??= new(SliceComparer.Instance);
                    batches[shardNumber].AttachmentStreams.TryAdd(attachmentReplicationItem.Base64Hash, null);
                }
            }

            if (dataForReplicationCommand.ReplicatedAttachmentStreams != null)
            {
                foreach (var attachment in dataForReplicationCommand.ReplicatedAttachmentStreams)
                {
                    if (attachment.Value.Stream is StreamsTempFile.InnerStream innerStream == false)
                        throw new NotSupportedException($"Cannot understand how to read stream of type '{attachment.Value.Stream.GetType().Name}'. Should not happen!");

                    foreach (var batch in batches.Values)
                    {
                        if (batch.AttachmentStreams?.ContainsKey(attachment.Key) != true)
                            continue;

                        var disposable = innerStream.CreateReaderStream(out var readerStream);
                        var attachmentStream = new AttachmentReplicationItem
                        {
                            Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                            Base64Hash = attachment.Key,
                            Stream = readerStream
                        };

                        attachmentStream.ToDispose(disposable);
                        batch.AttachmentStreams[attachment.Key] = attachmentStream;
                    }
                }
            }

            EnsureNotNullAttachmentStreams(batches);

            return replicationBatches;
        }

        private void EnsureNotNullAttachmentStreams(Dictionary<int, ReplicationBatch> batches)
        {
            foreach(var batch in batches.Values)
            {
                if (batch.AttachmentStreams != null)
                {
                    List<Slice> itemsToRemove = null;
                    foreach (var att in batch.AttachmentStreams)
                    {
                        if (att.Value == null)
                        {
                            itemsToRemove ??= new List<Slice>();
                            itemsToRemove.Add(att.Key);
                        }
                    }

                    if (itemsToRemove != null)
                    {
                        foreach (var key in itemsToRemove)
                            batch.AttachmentStreams.Remove(key);
                    }
                }
            }
        }

        public override LiveReplicationPerformanceCollector.ReplicationPerformanceType GetReplicationPerformanceType()
        {
            return LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingExternal;
        }

        protected override ByteStringContext GetContextAllocator(TransactionOperationContext context) => context.Allocator;

        protected override RavenConfiguration GetConfiguration() => _parent.Context.Configuration;

        protected override int GetNextReplicationStatsId() => _parent.GetNextReplicationStatsId();


        private readonly DocumentInfoHelper _documentInfoHelper = new DocumentInfoHelper();

        private int GetShardNumberForReplicationItem(TransactionOperationContext context, ReplicationBatchItem item)
        {
            switch (item)
            {
                case AttachmentReplicationItem:
                case AttachmentTombstoneReplicationItem:
                case CounterReplicationItem:
                case DocumentReplicationItem:
                case TimeSeriesDeletedRangeItem:
                case TimeSeriesReplicationItem:
                case RevisionTombstoneReplicationItem:
                    var id = _documentInfoHelper.GetDocumentId(item);
                    if (string.IsNullOrEmpty(id))
                        throw new ArgumentException("Document id cannot be null or empty", nameof(id));

                    return _parent.Context.GetShardNumberFor(context, id);
                default:
                    throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}");
            }
        }

        protected override void DisposeInternal()
        {
            Parallel.ForEach(_handlers, instance =>
            {
                try
                {
                    instance.Value?.Dispose();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"Failed to dispose sharded outgoing replication to {instance.Value?.DestinationFormatted}", e);
                    }
                }
            });

            base.DisposeInternal();
        }

        public class ReplicationBatches : IDisposable
        {
            public Dictionary<int, ReplicationBatch> Batches;

            public ReplicationBatches(ShardedIncomingReplicationHandler parent)
            {
                Batches = new Dictionary<int, ReplicationBatch>();
                foreach (var shardNumber in parent._parent.Context.ShardsTopology.Keys)
                    Batches[shardNumber] = new ReplicationBatch();
            }

            public void Dispose()
            {
                foreach (var batch in Batches.Values)
                {
                    batch.Dispose();
                }
                Batches = null;
            }
        }
    }
}
