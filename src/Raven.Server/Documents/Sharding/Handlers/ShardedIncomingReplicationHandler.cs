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
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
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

        protected override void InvokeOnFailed(Exception exception)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle replication failures");
        }

        protected override void HandleHeartbeatMessage(TransactionOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Find a way to implement change vector updates for tackling optimization issues");
        }

        protected override void InvokeOnDocumentsReceived()
        {
        }

        protected override void InvokeOnAttachmentStreamsReceived(int attachmentStreamCount)
        {
        }

        protected override void HandleTaskCompleteIfNeeded()
        {
            _replicationQueue.SendToShardCompletion = new AsyncCountdownEvent(_parent.Context.ShardCount);
        }

        protected override Task HandleBatchAsync(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand, long lastEtag)
        {
            PrepareReplicationDataForShards(context, dataForReplicationCommand);
            return _replicationQueue.SendToShardCompletion.WaitAsync();
        }

        protected override void HandleMissingAttachmentsIfNeeded(ref Task task)
        {
            if (_replicationQueue.MissingAttachments.IsSet)
            {
                task = null;
                _replicationQueue.MissingAttachments.Reset();

                throw new MissingAttachmentException(_replicationQueue.MissingAttachmentMessage);
            }
        }

        private void PrepareReplicationDataForShards(TransactionOperationContext context, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization possibility: instead of iterating over materialized batch, we can do it while reading from the stream");

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

                        if (shardAttachments == null || shardAttachments.ContainsKey(attachment.Key) == false)
                            continue;

                        var buffer = GetBufferFromAttachmentStream(attachment.Value.Stream);

                        var attachmentStream = new AttachmentReplicationItem
                        {
                            Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                            Base64Hash = attachment.Key,
                            Stream = new MemoryStream(buffer)
                        };

                        shardAttachments[attachment.Key] = attachmentStream;

                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization required");
                    }
                }
            }

            foreach (var kvp in dictionary)
            {
                var shard = kvp.Key;
                _replicationQueue.Items[shard].TryAdd(kvp.Value);
            }
        }

        private byte[] GetBufferFromAttachmentStream(Stream stream)
        {
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
    }
}
