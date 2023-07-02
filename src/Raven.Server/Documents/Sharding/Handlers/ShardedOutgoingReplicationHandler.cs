using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOutgoingReplicationHandler : AbstractOutgoingReplicationHandler<TransactionContextPool, TransactionOperationContext>
    {
        private readonly string _sourceDatabaseId;
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private readonly BlockingCollection<ReplicationBatch> _batches = new();
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private long _lastEtag;
        private string _lastAcceptedChangeVectorFromShard;
        private readonly TaskCompletionSource<(string, long)> _firstChangeVector = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public readonly string DestinationDatabaseName;
        public string MissingAttachmentMessage { get; set; }

        public ShardedOutgoingReplicationHandler(ShardedDatabaseContext.ShardedReplicationContext parent, ShardReplicationNode node, TcpConnectionInfo connectionInfo, string sourceDatabaseId) :
            base(connectionInfo, parent.Server, parent.Context.DatabaseName, parent.Context.NotificationCenter, node, parent.Server.ContextPool, parent.Context.DatabaseShutdown)
        {
            _parent = parent;
            _tcpConnectionOptions = new TcpConnectionOptions
            {
                DatabaseContext = _parent.Context,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication
            };
            _sourceDatabaseId = sourceDatabaseId;

            DestinationDatabaseName = node.Database;
        }

        protected override void Replicate()
        {
            ReplicationBatch batch = null;
            try
            {
                while (_cts.Token.IsCancellationRequested == false)
                {
                    batch = _batches.Take(_cts.Token);

                    using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                        AddReplicationPerformance(stats);

                        _lastEtag = _lastSentDocumentEtag;

                        using (var scope = stats.CreateScope())
                        {
                            EnsureValidStats(scope);

                            using (_stats.Network.Start())
                            {
                                SendDocumentsBatch(context, batch, _stats.Network);
                                batch.BatchSent.TrySetResult();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                batch?.BatchSent?.TrySetException(e);
                throw;
            }
        }

        private void SendDocumentsBatch(TransactionOperationContext context, ReplicationBatch batch, OutgoingReplicationStatsScope stats)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Unify this with the ReplicationDocumentSenderBase.SendDocumentsBatch");

            if (batch.LastSentEtagFromSource > _lastEtag)
                _lastEtag = _lastSentDocumentEtag = batch.LastSentEtagFromSource;

            if (batch.Items.Count == 0)
            {
                SendHeartbeat(batch.LastAcceptedChangeVector ?? _lastAcceptedChangeVectorFromShard);
                batch.LastAcceptedChangeVector = LastAcceptedChangeVector;
                return;
            }

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                [nameof(ReplicationMessageHeader.ItemsCount)] = batch.Items.Count,
                [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = batch.AttachmentStreams?.Count ?? 0,
            };

            WriteToServer(headerJson);

            stats.RecordLastEtag(_lastEtag);

            foreach (var item in batch.Items)
            {
                using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
                {
                    item.Write(cv, _stream, _tempBuffer, stats);
                }

                _lastEtag = item.Etag;
            }

            if (batch.AttachmentStreams != null)
            {
                foreach (var kvp in batch.AttachmentStreams)
                {
                    using (stats.For(ReplicationOperation.Outgoing.AttachmentRead))
                    {
                        var attachment = kvp.Value;
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

            var (type, reply) = HandleServerResponse(getFullResponse: true);

            batch.LastAcceptedChangeVector = _lastAcceptedChangeVectorFromShard = reply.DatabaseChangeVector;
            batch.LastEtagAccepted = _lastSentDocumentEtag = reply.LastEtagAccepted;

            MissingAttachmentMessage = type == ReplicationMessageReply.ReplyType.MissingAttachments ?
                reply?.Exception ?? "Unknown missing attachment message" :
                null;
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
            if (_firstChangeVector.Task.IsCompleted == false)
            {
                _firstChangeVector.TrySetResult((replicationBatchReply.DatabaseChangeVector, replicationBatchReply.LastEtagAccepted));
            }
        }

        public Task<(string AcceptedChangeVector, long LastAcceptedEtag)> GetFirstChangeVectorFromShardAsync() => _firstChangeVector.Task;

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var initialRequest = base.GetInitialHandshakeRequest();

            initialRequest[nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _sourceDatabaseId;
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
            _firstChangeVector.TrySetCanceled();
            _batches.CompleteAdding();
            foreach (var batch in _batches)
            {
                batch.BatchSent.TrySetCanceled();
            }
        }

        protected override void OnSuccessfulTwoWaysCommunication()
        {
            MissingAttachmentsRetries = 0;
        }

        protected override void OnFailed(Exception e)
        {
            _firstChangeVector.TrySetException(e);
            _batches.CompleteAdding();
            foreach (var batch in _batches)
            {
                batch.BatchSent.TrySetException(e);
            }
        }

        protected override long GetLastHeartbeatTicks() => _parent.Context.Time.GetUtcNow().Ticks;

        public Task SendBatch(ReplicationBatch batch)
        {
            batch.BatchSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _batches.Add(batch);
            return batch.BatchSent.Task;
        }
    }
}

