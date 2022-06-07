using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
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

        protected override void ReceiveSingleDocumentsBatch(TransactionOperationContext context, int replicatedItemsCount, int attachmentStreamCount, long lastEtag, IncomingReplicationStatsScope stats)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using (_attachmentStreamsTempFile.Scope())
                using (var shardAllocator = new IncomingReplicationAllocator(context.Allocator, maxSizeToSend: null))
                using (var dataForReplicationCommand = new IncomingReplicationHandler.DataForReplicationCommand
                {
                    SupportedFeatures = SupportedFeatures,
                    Logger = Logger
                })
                {
                    try
                    {
                        using (var networkStats = stats.For(ReplicationOperation.Incoming.Network))
                        {
                            var reader = new Reader(_stream, _copiedBuffer, shardAllocator);

                            ReadItemsFromSource(replicatedItemsCount, context, context.Allocator, dataForReplicationCommand, reader, networkStats);
                            ReadAttachmentStreamsFromSource(attachmentStreamCount, context, context.Allocator, dataForReplicationCommand, reader, networkStats);
                            PrepareReplicationDataForShards(context, dataForReplicationCommand);
                            
                            _replicationQueue.SendToShardCompletion.Wait();
                            _replicationQueue.SendToShardCompletion = new CountdownEvent(_parent.Context.ShardCount);
                        }
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            //This is the case where we had a missing attachment, it is rare but expected.
                            if (e.ExtractSingleInnerException() is MissingAttachmentException mae)
                            {
                                Logger.Info("Replication batch contained missing attachments will request the batch to be re-sent with those attachments.", mae);
                            }
                            else
                            {
                                Logger.Info("Failed to receive documents replication batch. This is not supposed to happen, and is likely a bug.", e);
                            }
                        }
                        throw;
                    }
                }

                sw.Stop();
            }
            catch (Exception)
            {
                // ignore this failure, if this failed, we are already
                // in a bad state and likely in the process of shutting
                // down
            }
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

        internal override void SendHeartbeatStatusToSource(TransactionOperationContext context, BlittableJsonTextWriter writer, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = "Ok",
                [nameof(ReplicationMessageReply.MessageType)] = handledMessageType,
                [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastDocumentEtag,
                [nameof(ReplicationMessageReply.Exception)] = null,
                [nameof(ReplicationMessageReply.NodeTag)] = _parent.Server.NodeTag
            };

            context.Write(writer, heartbeat);
            writer.Flush();
        }

        protected override void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null)
        {
        }

        protected override int GetNextReplicationStatsId() => _parent.GetNextReplicationStatsId();


        private readonly DocumentInfoHelper _documentInfoHelper = new DocumentInfoHelper();
        private LazyStringValue GetDocumentId(Slice key) => _documentInfoHelper.GetDocumentId(key);
        public string GetItemInformation(ReplicationBatchItem item) => _documentInfoHelper.GetItemInformation(item);

        public int GetShardNumberForReplicationItem(TransactionOperationContext context, ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => _parent.Context.GetShardNumber(context, (GetDocumentId(a.Key))),
                AttachmentTombstoneReplicationItem at => _parent.Context.GetShardNumber(context, (GetDocumentId(at.Key))),
                CounterReplicationItem c => _parent.Context.GetShardNumber(context, c.Id),
                DocumentReplicationItem d => _parent.Context.GetShardNumber(context, d.Id),
                RevisionTombstoneReplicationItem _ => throw new NotSupportedInShardingException("TODO: implement for sharding"), // revision tombstones doesn't contain any info about the doc. The id here is the change-vector of the deleted revision
                TimeSeriesDeletedRangeItem td => _parent.Context.GetShardNumber(context, (GetDocumentId(td.Key))),
                TimeSeriesReplicationItem t => _parent.Context.GetShardNumber(context, (GetDocumentId(t.Key))),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }
    }
}
