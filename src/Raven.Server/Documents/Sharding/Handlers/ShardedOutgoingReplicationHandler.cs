using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
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
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOutgoingReplicationHandler : AbstractOutgoingReplicationHandler<TransactionContextPool, TransactionOperationContext>
    {
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private ManualResetEvent _hasBatch = new(false);
        private ReplicationBatch _batch;
        private TaskCompletionSource _tcs;

        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private long _lastEtag;

        internal DateTime _lastDocumentSentTime;
        internal long LastEtag => _lastEtag;
        public string MissingAttachmentMessage { get; set; }
        public bool MissingAttachmentsInLastBatch { get; set; }
        public string LastDatabaseChangeVector { get; set; }
        public string SourceDatabaseId;
        public long CurrentEtag { get; set; }

        public ShardedOutgoingReplicationHandler(ShardedDatabaseContext.ShardedReplicationContext parent, ShardReplicationNode node, TcpConnectionInfo connectionInfo, string sourceDatabaseId) :
            base(connectionInfo, parent.Server, parent.Context.DatabaseName, node, parent.Context.DatabaseShutdown, parent.Server.ContextPool)
        {
            _parent = parent;
            _tcpConnectionOptions = new TcpConnectionOptions
            {
                DatabaseContext = _parent.Context,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication
            };

            SourceDatabaseId = sourceDatabaseId;
        }

        protected override void Replicate()
        {
            while (_cts.Token.IsCancellationRequested == false)
            {
                var hasBatch = _hasBatch.WaitOne(3000); //TODO: time for that
                _hasBatch.Reset();
                using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                    AddReplicationPerformance(stats);

                    using (var scope = stats.CreateScope())
                    {
                        EnsureValidStats(scope);

                        if (hasBatch == false)
                        {
                            _lastSentDocumentEtag = _lastEtag;
                            _lastDocumentSentTime = DateTime.UtcNow;

                            SendHeartbeat(LastDatabaseChangeVector);
                            continue;
                        }

                        var batch = _batch;
                        var tcs = _tcs;
                        MissingAttachmentsInLastBatch = false;

                        try
                        {
                            using (_stats.Network.Start())
                            {
                                if (batch.Items.Count > 0)
                                {
                                    _lastEtag = batch.Items.Last().Etag;
                                }
                                
                                var didWork = SendDocumentsBatch(context, batch, _stats.Network);
                                _tcpConnectionOptions._lastEtagSent = _lastEtag;
                                tcs.TrySetResult();
                            }
                        }
                        catch (Exception e)
                        {
                            tcs.TrySetException(e);
                            return;
                            //TODO: is the connection still alive, need to abort?
                        }
                    }
                }
            }
        }

        private bool SendDocumentsBatch(TransactionOperationContext context, ReplicationBatch batch, OutgoingReplicationStatsScope stats)
        {
            try
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "Unify this with the ReplicationDocumentSenderBase.SendDocumentsBatch");

                var sw = Stopwatch.StartNew();
                var headerJson = new DynamicJsonValue
                {
                    [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                    [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                    [nameof(ReplicationMessageHeader.ItemsCount)] = batch.Items.Count,
                    [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = batch.Attachments?.Count ?? 0,
                };

                WriteToServer(headerJson);

                foreach (var item in batch.Items)
                {
                    using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
                    {
                        item.Write(cv, _stream, _tempBuffer, stats);
                    }
                }

                if (batch.Attachments != null)
                {
                    foreach (var kvp in batch.Attachments)
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
                }

                _stream.Flush();
                sw.Stop();

                var (type, reply) = HandleServerResponse(getFullResponse: true);

                if (type == ReplicationMessageReply.ReplyType.MissingAttachments)
                {
                    MissingAttachmentsInLastBatch = true;
                    MissingAttachmentMessage = reply?.Exception;
                    return false;
                }

                _lastSentDocumentEtag = _lastEtag;
                _lastDocumentSentTime = DateTime.UtcNow;
            }
            finally
            {
                foreach (var item in batch.Items)
                {
                    item.Dispose();
                }
            }

            return true;
        }

        internal override ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage, bool allowNotify)
        {
            var replicationBatchReply = base.HandleServerResponse(replicationBatchReplyMessage, allowNotify);
            if (replicationBatchReply != null)
            {
                LastDatabaseChangeVector = replicationBatchReply.DatabaseChangeVector;
                CurrentEtag = replicationBatchReply.CurrentEtag;
            }
                
            return replicationBatchReply;
        }

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var initialRequest = base.GetInitialHandshakeRequest();

            initialRequest[nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = SourceDatabaseId;
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

        public Task SendBatch(ReplicationBatch batch)
        {
            _batch = batch;
            var taskCompletionSource = new TaskCompletionSource();
            _tcs = taskCompletionSource;
            _hasBatch.Set();
            return taskCompletionSource.Task;
        }
    }
}

