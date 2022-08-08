using System;
using System.IO;
using System.Threading.Tasks;
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

        public long LastEtag { get; set; }
        public string LastReplicateChangeVector { get; set; }

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag)
            : base(tcpConnectionOptions, buffer, parent.Server, parent.DatabaseName, replicatedLastEtag, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _attachmentStreamsTempFile = new StreamsTempFile("ShardedReplication" + Guid.NewGuid(), false);
            var connectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);

            _handlers = new ShardedOutgoingReplicationHandler[_parent.Context.ShardCount];
            for (int i = 0; i < _handlers.Length; i++)
            {
                var node = new ShardReplicationNode { Shard = i, Database = ShardHelper.ToShardName(_parent.DatabaseName, i) };
                var info = GetConnectionInfo(node);
          
                _handlers[i] = new ShardedOutgoingReplicationHandler(parent, node, info, connectionInfo.SourceDatabaseId);
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
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Find a way to implement change vector updates for tackling optimization issues");
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(TransactionOperationContext context, long lastDocumentEtag, string handledMessageType)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement this");
          
            return base.GetHeartbeatStatusMessage(context, lastDocumentEtag, handledMessageType);
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
