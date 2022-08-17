using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NCrontab.Advanced.Extensions;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
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
        private readonly ShardedOutgoingReplicationHandler[] _handlers;

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag)
            : base(tcpConnectionOptions, buffer, parent.Server, parent.DatabaseName, replicatedLastEtag, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _attachmentStreamsTempFile = new StreamsTempFile("ShardedReplication" + Guid.NewGuid(), false);
            _handlers = new ShardedOutgoingReplicationHandler[_parent.Context.ShardCount];

            for (int i = 0; i < _handlers.Length; i++)
            {
                var node = new ShardReplicationNode { Shard = i, Database = ShardHelper.ToShardName(_parent.DatabaseName, i) };
                var info = GetConnectionInfo(node);
          
                _handlers[i] = new ShardedOutgoingReplicationHandler(parent, node, info, replicatedLastEtag.SourceDatabaseId);
                _handlers[i].Start();
            }
        }

        private TcpConnectionInfo GetConnectionInfo(ShardReplicationNode node)
        {
            var shardExecutor = _parent.Context.ShardExecutor;
            using (_parent.Context.AllocateContext(out JsonOperationContext ctx))
            {
                var cmd = new GetTcpInfoCommand("sharded-replication", node.Database);
                RequestExecutor requestExecutor = null;
                try
                {
                    requestExecutor = shardExecutor.GetRequestExecutorAt(node.Shard);
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

        protected override void EnsureNotDeleted(string nodeTag) => _parent.EnsureNotDeleted(_parent.Server.NodeTag);

        protected override void InvokeOnFailed(Exception exception)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle replication failures");
        }

        protected override void HandleHeartbeatMessage(TransactionOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject)
        {
            if (blittableJsonReaderObject.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector))
            {
                foreach (var handler in _handlers)
                {
                    handler._lastSourceChangeVector = changeVector;
                    handler._lastDocumentEtagFromSource = _lastDocumentEtag;
                }
            }
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(TransactionOperationContext context, long lastDocumentEtag, string handledMessageType)
        {
            var handlersChangeVector = new List<string>();
            foreach (var handler in _handlers)
            {
                if (handler._lastDatabaseChangeVector.IsNullOrWhiteSpace() == false)
                    handlersChangeVector.Add(handler._lastDatabaseChangeVector);
            }
            var mergedChangeVector = ChangeVectorUtils.MergeVectors(handlersChangeVector);

            var heartbeat = base.GetHeartbeatStatusMessage(context, lastDocumentEtag, handledMessageType);
            heartbeat[nameof(ReplicationMessageReply.DatabaseChangeVector)] = mergedChangeVector;
            return heartbeat;
        }

        internal (string LastAcceptedChangeVector, long LastEtagFromSource) GetInitialHandshakeResponseFromShards()
        {
            // this is an optimization for replication failovers.
            // in case of failover, send back to the source the last change vector & Etag received on shards
            // so the replication won't start from scratch

            long lastEtagFromSource = 0;
            var handlersChangeVector = new List<string>();
            foreach (var handler in _handlers)
            {
                var lastReplicateEtag = handler._lastDocumentEtagFromSource;
                lastEtagFromSource = Math.Max(lastEtagFromSource, lastReplicateEtag);

                if (handler._lastDatabaseChangeVector.IsNullOrWhiteSpace() == false)
                    handlersChangeVector.Add(handler._lastDatabaseChangeVector);
            }
            var mergedChangeVector = ChangeVectorUtils.MergeVectors(handlersChangeVector);
            return (mergedChangeVector, lastEtagFromSource);
        }

        protected override void InvokeOnDocumentsReceived()
        {
        }

        protected override void InvokeOnAttachmentStreamsReceived(int attachmentStreamCount)
        {
        }

        protected override async Task HandleBatchAsync(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand, long lastEtag)
        {
            var batches = PrepareReplicationDataForShards(context, dataForReplicationCommand);
            var tasks = new Task[batches.Length];
            for (int i = 0; i < batches.Length; i++)
            {
                _handlers[i]._lastDocumentEtagFromSource = lastEtag;
                tasks[i] = _handlers[i].SendBatch(batches[i]);
            }

            await Task.WhenAll(tasks);

            foreach (ShardedOutgoingReplicationHandler handler in _handlers)
            {
                if (handler.MissingAttachmentsInLastBatch)
                    throw new MissingAttachmentException(handler.MissingAttachmentMessage);
            }
        }

        private ReplicationBatch[] PrepareReplicationDataForShards(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization possibility: instead of iterating over materialized batch, we can do it while reading from the stream");

            var batches = new ReplicationBatch[_parent.Context.ShardCount];
            for (int i = 0; i <  _parent.Context.ShardCount; i++)
            {
                batches[i] = new();
            }

            foreach (var item in dataForReplicationCommand.ReplicatedItems)
            {
                int shard = GetShardNumberForReplicationItem(context, item);
                batches[shard].Items.Add(item);
                if (item is AttachmentReplicationItem attachmentReplicationItem)
                {
                    batches[shard].Attachments ??= new(SliceComparer.Instance);
                    batches[shard].Attachments.Add(attachmentReplicationItem.Base64Hash, null);
                }
            }

            if (dataForReplicationCommand.ReplicatedAttachmentStreams != null)
            {
                foreach (var attachment in dataForReplicationCommand.ReplicatedAttachmentStreams)
                {
                    for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
                    {
                        if(batches[shard].Attachments?.ContainsKey(attachment.Key) != true)
                            continue;
                        
                        var buffer = GetBufferFromAttachmentStream(attachment.Value.Stream);

                        var attachmentStream = new AttachmentReplicationItem
                        {
                            Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                            Base64Hash = attachment.Key,
                            Stream = new MemoryStream(buffer)
                        };

                        batches[shard].Attachments[attachment.Key] = attachmentStream;

                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization required");
                    }
                }
            }

            return batches;
        }

        private byte[] GetBufferFromAttachmentStream(Stream stream)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Avoid this");

            var length = (int)stream.Length;
            var buffer = new byte[length];

            stream.Position = 0;
            int read = stream.Read(buffer, 0, buffer.Length);
            stream.Position = 0;

            return buffer;
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
                    return _parent.Context.GetShardNumber(context, _documentInfoHelper.GetDocumentId(item));
                case RevisionTombstoneReplicationItem:
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle this (document Id does not exist)");
                    throw new NotSupportedInShardingException("TODO: implement for sharding"); // revision tombstones doesn't contain any info about the doc. The id here is the change-vector of the deleted revision
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
                    instance?.Dispose();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"Failed to dispose sharded outgoing replication to {instance?.DestinationFormatted}", e);
                    }
                }
            });

            base.DisposeInternal();
        }
    }
}
