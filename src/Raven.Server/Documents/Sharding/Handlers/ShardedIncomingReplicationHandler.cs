using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Nito.AsyncEx.Synchronous;
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
using Raven.Server.Indexing;
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
        private readonly ReplicationBatches _batches;
        private readonly TempFileCache _tempFileCache;
        private string _lastAcceptedChangeVector;
        private long _lastAcceptedEtag;

        public ShardedIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ShardedDatabaseContext.ShardedReplicationContext parent,
            JsonOperationContext.MemoryBuffer buffer, ReplicationLatestEtagRequest replicatedLastEtag)
            : base(tcpConnectionOptions, buffer, parent.Server, parent.DatabaseName, replicatedLastEtag, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _tempFileCache = new TempFileCache(parent.Server.Configuration.Storage.TempPath?.FullPath ?? Path.GetTempPath(), encrypted: false); // TODO: figure out if the sharded db is encrypted
            _attachmentStreamsTempFile = new StreamsTempFile("ShardedReplication" + Guid.NewGuid(), false);
            _handlers = new ShardedOutgoingReplicationHandler[_parent.Context.ShardCount];
            _batches = new ReplicationBatches(this)
            {
                Batches = new ReplicationBatch[parent.Context.ShardCount]
            };
            for (int i = 0; i < _batches.Batches.Length; i++)
            {
                _batches.Batches[i] = new ReplicationBatch();
            }

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

        protected override void InvokeOnFailed(Exception exception) => _parent.InvokeOnFailed(this, exception);

        protected override void HandleHeartbeatMessage(TransactionOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject)
        {
            blittableJsonReaderObject.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector);

            using var replicationBatches = _batches;
            var batches = replicationBatches.Batches;
            var tasks = new Task[batches.Length];

            for (int i = 0; i < batches.Length; i++)
            {
                batches[i].LastAcceptedChangeVector = changeVector ?? _lastAcceptedChangeVector;
                batches[i].LastSentEtagFromSource = _lastAcceptedEtag;
                tasks[i] = _handlers[i].SendBatch(batches[i]);
            }

            Task.WaitAll(tasks);

            var cvs = new List<string>();
            foreach (ReplicationBatch replicationBatch in batches)
            {
                cvs.Add(replicationBatch.LastAcceptedChangeVector);
            }

            _lastAcceptedChangeVector = ChangeVectorUtils.MergeVectorsDown(cvs);
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
            foreach (var handler in _handlers)
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
            using var replicationBatches = PrepareReplicationDataForShards(context, dataForReplicationCommand);
            var batches = replicationBatches.Batches;
            var tasks = new Task[batches.Length];
            for (int i = 0; i < batches.Length; i++)
            {
                batches[i].LastSentEtagFromSource = _lastDocumentEtag;
                tasks[i] = _handlers[i].SendBatch(batches[i]);
            }

            await Task.WhenAll(tasks);

            foreach (ShardedOutgoingReplicationHandler handler in _handlers)
            {
                if (handler.MissingAttachmentsInLastBatch)
                    throw new MissingAttachmentException(handler.MissingAttachmentMessage);
            }

            var cvs = new List<string>();
            var minEtag = long.MaxValue;
            foreach (ReplicationBatch replicationBatch in batches)
            {
                cvs.Add(replicationBatch.LastAcceptedChangeVector);
                minEtag = Math.Min(minEtag, replicationBatch.LastEtagAccepted);
            }

            _lastAcceptedChangeVector = ChangeVectorUtils.MergeVectorsDown(cvs);
            _lastAcceptedEtag = minEtag;

        }

        private ReplicationBatches PrepareReplicationDataForShards(TransactionOperationContext context, DataForReplicationCommand dataForReplicationCommand)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Optimization possibility: instead of iterating over materialized batch, we can do it while reading from the stream");
            var batches = _batches.Batches;

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
                    var stream = attachment.Value.Size > _tempFileCache.MemoryStreamCapacity /* TODO: Make configurable */
                        ? _tempFileCache.RentFileStream()
                        : _tempFileCache.RentMemoryStream();

                    _batches.Streams.Add(stream);

                    attachment.Value.Stream.Seek(0, SeekOrigin.Begin);
                    attachment.Value.Stream.CopyTo(stream);

                    for (var shard = 0; shard < _parent.Context.ShardCount; shard++)
                    {
                        if (batches[shard].Attachments?.ContainsKey(attachment.Key) != true)
                            continue;

                        var attachmentStream = new AttachmentReplicationItem
                        {
                            Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                            Base64Hash = attachment.Key,
                            Stream = stream switch
                            {
                                MemoryStream ms => new MemoryStream(ms.GetBuffer(), 0, (int)ms.Length),
                                TempFileStream tmp => tmp.CreateReaderStream(),
                                _ => throw new NotSupportedException("Cannot understand how to clone stream: " + stream)
                            }
                        };

                        batches[shard].Attachments[attachment.Key] = attachmentStream;
                    }
                }
            }

            return _batches;
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
            _tempFileCache.Dispose();
        }

        public class ReplicationBatches : IDisposable
        {
            private readonly ShardedIncomingReplicationHandler _parent;
            public ReplicationBatch[] Batches;
            public List<Stream> Streams = new();

            public ReplicationBatches(ShardedIncomingReplicationHandler parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                foreach (var batch in Batches)
                {
                    batch.Dispose();
                }
                foreach (Stream stream in Streams)
                {
                    if (stream is MemoryStream ms)
                    {
                        _parent._tempFileCache.ReturnMemoryStream(ms);
                    }
                    else
                    {
                        _parent._tempFileCache.ReturnFileStream(stream);
                    }
                }
                Streams.Clear();
            }
        }
    }
}
