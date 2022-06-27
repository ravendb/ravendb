using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOutgoingReplicationHandler : AbstractOutgoingReplicationHandler<TransactionContextPool, TransactionOperationContext>
    {
        private readonly int _shardNumber;
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private readonly ReplicationQueue _replicationQueue;

        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private long _lastEtag;

        public ShardedOutgoingReplicationHandler(ShardedDatabaseContext.ShardedReplicationContext parent, ShardReplicationNode node, int shardNumber,
            TcpConnectionInfo connectionInfo, ReplicationQueue replicationQueue) :
            base(connectionInfo, parent.Server, parent.DatabaseName, node, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _shardNumber = shardNumber;

            _tcpConnectionOptions = new TcpConnectionOptions
            {
                DatabaseContext = parent.Context,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication
            };

            _replicationQueue = replicationQueue;
        }

        protected override void Replicate()
        {
            while (_cts.IsCancellationRequested == false)
            {
                while (_replicationQueue.Items[_shardNumber].TryTake(out var items))
                {
                    using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                        AddReplicationPerformance(stats);

                        using (var scope = stats.CreateScope())
                        {
                            EnsureValidStats(scope);

                            if (items.Count == 0)
                            {
                                SendHeartbeat(null);
                                _replicationQueue.SendToShardCompletion.Signal();
                                continue;
                            }

                            using (_stats.Network.Start())
                            {
                                MissingAttachmentsInLastBatch = false;

                                var didWork = SendDocumentsBatch(context, items, _stats.Network);

                                if (MissingAttachmentsInLastBatch)
                                {
                                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle missing attachments");
                                    continue;
                                }

                                _replicationQueue.SendToShardCompletion.Signal();

                                if (didWork == false)
                                    break;
                            }
                        }
                    }
                }
            }
        }

        private bool SendDocumentsBatch(TransactionOperationContext context, List<ReplicationBatchItem> items, OutgoingReplicationStatsScope stats)
        {
            try
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Unify this with the ReplicationDocumentSenderBase.SendDocumentsBatch");

                var sw = Stopwatch.StartNew();
                var headerJson = new DynamicJsonValue
                {
                    [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                    [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                    [nameof(ReplicationMessageHeader.ItemsCount)] = items.Count,
                    [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = _replicationQueue.AttachmentsPerShard[_shardNumber].Count
                };

                WriteToServer(headerJson);

                foreach (var item in items)
                {
                    using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
                    {
                        item.Write(cv, _stream, _tempBuffer, stats);
                    }

                    _lastEtag = item.Etag;
                }

                foreach (var kvp in _replicationQueue.AttachmentsPerShard[_shardNumber])
                {
                    using (stats.For(ReplicationOperation.Outgoing.AttachmentRead))
                    {
                        using (var attachment = kvp.Value)
                        {
                            try
                            {
                                attachment.WriteStream(_stream, _tempBuffer);
                                stats.RecordAttachmentOutput(attachment.Stream.Length);
                            }
                            catch
                            {
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"Failed to write Attachment stream {FromToString}");

                                throw;
                            }
                        }
                    }
                }

                _stream.Flush();
                sw.Stop();

                var (type, _) = HandleServerResponse();

                if (type == ReplicationMessageReply.ReplyType.MissingAttachments)
                {
                    MissingAttachmentsInLastBatch = true;
                    return false;
                }

                _lastSentDocumentEtag = _lastEtag;
            }
            finally
            {
                foreach (var item in items)
                {
                    item.Dispose();
                }

                _replicationQueue.AttachmentsPerShard[_shardNumber].Clear();
            }

            return true;
        }

        public bool MissingAttachmentsInLastBatch { get; set; }

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var initialRequest = base.GetInitialHandshakeRequest();

            initialRequest[nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _parent.SourceDatabaseId;
            return initialRequest;
        }

        protected override void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Add alert");
        }

        protected override void InitiatePullReplicationAsSink(TcpConnectionHeaderMessage.SupportedFeatures socketResultSupportedFeatures, X509Certificate2 certificate)
        {
            throw new NotSupportedInShardingException();
        }

        protected override void AssertDatabaseNotDisposed()
        {
            if (_parent.Context == null)
                throw new InvalidOperationException("Sharded database context got disposed. Stopping the replication.");
        }

        protected override X509Certificate2 GetCertificateForReplication(ReplicationNode destination, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            return _parent.GetCertificateForReplication(Destination, out authorizationInfo);
        }

        protected override void OnBeforeDispose()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle this");
        }

        protected override void OnSuccessfulTwoWaysCommunication()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle this");
        }

        protected override void OnFailed(Exception e)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Handle this");
        }

        protected override long GetLastHeartbeatTicks() => _parent.Context.Time.GetUtcNow().Ticks;
    }
}

