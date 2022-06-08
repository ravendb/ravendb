using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIncomingReplicationHandler : AbstractIncomingReplicationHandler<TransactionContextPool, TransactionOperationContext>
    {
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private readonly ReplicationQueue _replicationQueue;

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationQueue replicationQueue)
            : base(tcpConnectionOptions, buffer, parent.Server, parent.DatabaseName, replicatedLastEtag, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _replicationQueue = replicationQueue;
            _attachmentStreamsTempFile = new StreamsTempFile("ShardedReplication" + Guid.NewGuid(), false);
        }

        protected override void EnsureNotDeleted(string nodeTag) => _parent.EnsureNotDeleted(_parent.Server.NodeTag);

        protected override void OnFailed(Exception exception)
        {
        }

        protected override void HandleHeartbeatMessage(TransactionOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject)
        {
        }

        protected override void OnDocumentsReceived()
        {
        }

        protected override void OnAttachmentStreamsReceives(int attachmentStreamCount)
        {
        }

        protected override void HandleTaskCompleteIfNeeded()
        {
            _replicationQueue.SendToShardCompletion = new AsyncCountdownEvent(_parent.Context.ShardCount);
        }

        protected override Task HandleBatch(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand, long lastEtag)
        {
            PrepareReplicationDataForShards(context, dataForReplicationCommand);
            return _replicationQueue.SendToShardCompletion.WaitAsync();
        }

        private void PrepareReplicationDataForShards(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand)
        {
            var dictionary = new Dictionary<int, List<ReplicationBatchItem>>();
            for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
                dictionary[shard] = new List<ReplicationBatchItem>();

            foreach (var item in dataForReplicationCommand.ReplicatedItems)
            {
                int shard = GetShardNumberForReplicationItem(context, item);

                if (item is AttachmentReplicationItem attachment)
                {
                    var shardAttachments = _replicationQueue.AttachmentsPerShard[shard] ??= new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);

                    if (shardAttachments.ContainsKey(attachment.Base64Hash) == false)
                        shardAttachments[attachment.Base64Hash] = attachment;
                }

                dictionary[shard].Add(item);
            }

            if (dataForReplicationCommand.ReplicatedAttachmentStreams != null)
            {
                foreach (var attachment in dataForReplicationCommand.ReplicatedAttachmentStreams)
                {
                    for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
                    {
                        var shardAttachments = _replicationQueue.AttachmentsPerShard[shard];

                        if (shardAttachments == null)
                            continue;

                        if (shardAttachments.ContainsKey(attachment.Key))
                        {
                            var attachmentStream = new AttachmentReplicationItem
                            {
                                Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                                Base64Hash = attachment.Key,
                                Stream = new MemoryStream()
                            };

                            _attachmentStreamsTempFile._file.InnerStream.CopyTo(attachmentStream.Stream);
                            shardAttachments[attachment.Key] = attachmentStream;
                        }
                    }
                }
            }

            foreach (var kvp in dictionary)
            {
                var shard = kvp.Key;
                _replicationQueue.Items[shard].TryAdd(kvp.Value);
            }
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
                    throw new NotSupportedInShardingException("TODO: implement for sharding"); // revision tombstones doesn't contain any info about the doc. The id here is the change-vector of the deleted revision
                default:
                    throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}");

            }
        }
    }
}
