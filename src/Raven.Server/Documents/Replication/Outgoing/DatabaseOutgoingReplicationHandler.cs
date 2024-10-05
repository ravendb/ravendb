using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public abstract class DatabaseOutgoingReplicationHandler : AbstractOutgoingReplicationHandler<DocumentsContextPool, DocumentsOperationContext>
    {
        public const string AlertTitle = "Replication";

        internal readonly DocumentDatabase _database;
        protected readonly AsyncManualResetEvent _waitForChanges;
        internal readonly ReplicationLoader _parent;
        internal DateTime _lastDocumentSentTime;

        public event Action<DatabaseOutgoingReplicationHandler, Exception> Failed;

        public event Action<DatabaseOutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

        public event Action<DatabaseOutgoingReplicationHandler> SuccessfulReplication;

        public event Action<DatabaseOutgoingReplicationHandler> DocumentsSend;

        protected DatabaseOutgoingReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, TcpConnectionInfo connectionInfo)
        : base(connectionInfo, parent._server, database.Name, database.NotificationCenter, node, database.DocumentsStorage.ContextPool, database.DatabaseShutdown)
        {
            _parent = parent;
            _database = database;
            _waitForChanges = new AsyncManualResetEvent(_database.DatabaseShutdown);
            _tcpConnectionOptions = new TcpConnectionOptions
            {
                DocumentDatabase = database,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication
            };

            _database.Changes.OnDocumentChange += OnDocumentChange;
            _database.Changes.OnCounterChange += OnCounterChange;
            _database.Changes.OnTimeSeriesChange += OnTimeSeriesChange;
        }

        public override int GetHashCode()
        {
            return Destination.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DatabaseOutgoingReplicationHandler)obj);
        }

        public bool Equals(DatabaseOutgoingReplicationHandler other)
        {
            return Destination.Equals(other.Destination);
        }


        public LiveReplicationPerformanceCollector.ReplicationPerformanceType GetReplicationPerformanceType()
        {
            switch (this)
            {
                case OutgoingExternalReplicationHandler:
                    return LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingExternal;
                case OutgoingInternalReplicationHandler:
                    return LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingInternal;
                default:
                    return LiveReplicationPerformanceCollector.ReplicationPerformanceType.OutgoingExternal;
            }
        }

        public OutgoingReplicationStatsAggregator GetLatestReplicationPerformance()
        {
            return _lastStats;
        }

        public void StartPullReplicationAsHub(Stream stream, TcpConnectionHeaderMessage.SupportedFeatures supportedVersions)
        {
            SupportedFeatures = supportedVersions;
            _stream = stream;
            OutgoingReplicationThreadName = $"Pull replication as hub {FromToString}";
            _longRunningSendingWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleReplicationErrors(PullReplication), null, ThreadNames.ForOutgoingReplication(OutgoingReplicationThreadName,
                    _database.Name, Destination.FromString(), pullReplicationAsHub: true));
        }

        private void PullReplication()
        {
            NativeMemory.EnsureRegistered();

            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
            if (Logger.IsInfoEnabled)
                Logger.Info($"Start pull replication as hub {FromToString}");

            using (_stream)
            using (_interruptibleRead = new InterruptibleRead<DocumentsContextPool, DocumentsOperationContext>(_parent.ContextPool, _stream))
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out _buffer))
            {
                InitialHandshake();
                Replicate();
            }
        }

        protected override void AssertDatabaseNotDisposed()
        {
            var database = _parent.Database;
            if (database == null)
                throw new InvalidOperationException("The database got disposed. Stopping the replication.");
        }

        protected override X509Certificate2 GetCertificateForReplication(ReplicationNode destination, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            return _parent.GetCertificateForReplication(Destination, out authorizationInfo);
        }

        protected override void InitiatePullReplicationAsSink(TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures, X509Certificate2 certificate)
        {
            var tcpOptions = new TcpConnectionOptions
            {
                ContextPool = _parent._server.Server._tcpContextPool,
                Stream = _stream,
                TcpClient = _tcpClient,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,
                DocumentDatabase = _database,
                ProtocolVersion = supportedFeatures.ProtocolVersion,
                Certificate = certificate
            };

            try
            {
                using (_parent._server.Server._tcpContextPool.AllocateOperationContext(out var ctx))
                using (ctx.GetMemoryBuffer(out _buffer))
                {
                    _parent.RunPullReplicationAsSink(tcpOptions, _buffer, Destination as PullReplicationAsSink, this);
                }
            }
            catch
            {
                try
                {
                    tcpOptions.Dispose();
                }
                catch
                {
                    // nothing we can do
                }

                throw;
            }
        }

        protected override void HandleReplicationErrors(Action replicationAction)
        {
            _parent.ForTestingPurposes?.OnOutgoingReplicationStart?.Invoke(this);
            base.HandleReplicationErrors(replicationAction);
        }

        public long NextReplicateTicks;

        public abstract ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, RavenLogger logger);

        protected override void Replicate()
        {
            using var documentSender = CreateDocumentSender(_stream, Logger);

            while (_cts.IsCancellationRequested == false)
            {
                while (_database.Time.GetUtcNow().Ticks > NextReplicateTicks)
                {
                    var once = _parent.DebugWaitAndRunReplicationOnce;
                    if (once != null)
                    {
                        once.Reset();
                        once.Wait(_cts.Token);
                    }

                    var sp = Stopwatch.StartNew();
                    var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                    AddReplicationPerformance(stats);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingBegin);

                    try
                    {
                        using (var scope = stats.CreateScope())
                        {
                            try
                            {
                                if (Destination is InternalReplication dest)
                                {
                                    _parent.EnsureNotDeleted(dest.NodeTag);
                                }

                                var didWork = documentSender.ExecuteReplicationOnce(_tcpConnectionOptions, scope, ref NextReplicateTicks);
                                if (documentSender.MissingAttachmentsInLastBatch)
                                    continue;

                                if (didWork == false)
                                    break;

                                DocumentsSend?.Invoke(this);

                                if (sp.ElapsedMilliseconds > 60 * 1000)
                                {
                                    _waitForChanges.Set();
                                    break;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // cancellation is not an actual error,
                                // it is a "notification" that we need to cancel current operation

                                const string msg = "Operation was canceled.";
                                AddReplicationPulse(ReplicationPulseDirection.OutgoingError, msg);

                                throw;
                            }
                            catch (Exception e)
                            {
                                scope.AddError(e);
                                AddReplicationPulse(ReplicationPulseDirection.OutgoingError, e.Message);
                                throw;
                            }
                        }
                    }
                    finally
                    {
                        stats.Complete();
                        AddReplicationPulse(ReplicationPulseDirection.OutgoingEnd);
                    }
                }

                OnSuccessfulReplication();

                //if this returns false, this means either timeout or canceled token is activated
                while (WaitForChanges(_parent.MinimalHeartbeatInterval, _cts.Token) == false)
                {
                    //If we got cancelled we need to break right away
                    if (_cts.IsCancellationRequested)
                        break;

                    // open tx
                    // read current change vector compare to last sent
                    // if okay, send cv
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (var tx = ctx.OpenReadTransaction())
                    {
                        var etag = _database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        if (etag == _lastSentDocumentEtag)
                        {
                            SendHeartbeat(DocumentsStorage.GetDatabaseChangeVector(ctx));
                            _parent.CompleteDeletionIfNeeded(_cts);
                        }
                        else if (NextReplicateTicks > DateTime.UtcNow.Ticks)
                        {
                            SendHeartbeat(null);
                        }
                        else
                        {
                            //Send a heartbeat first so we will get an updated CV of the destination
                            var currentChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                            SendHeartbeat(null);
                            //If our previous CV is already merged to the destination wait a bit more
                            if (ChangeVectorUtils.GetConflictStatus(LastAcceptedChangeVector, currentChangeVector) ==
                                ConflictStatus.AlreadyMerged)
                            {
                                continue;
                            }

                            // we have updates that we need to send to the other side
                            // let's do that..
                            // this can happen if we got replication from another node
                            // that we need to send to it. Note that we typically
                            // will wait for the other node to send the data directly to
                            // our destination, but if it doesn't, we'll step in.
                            // In this case, we try to limit congestion in the network and
                            // only send updates that we have gotten from someone else after
                            // a certain time, to let the other side tell us that it already
                            // got it. Note that this is merely an optimization to reduce network
                            // traffic. It is fine to have the same data come from different sources.
                            break;
                        }
                    }
                }

                _waitForChanges.Reset();
            }
        }

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var initialRequest = base.GetInitialHandshakeRequest();

            initialRequest[nameof(ReplicationLatestEtagRequest.SourceDatabaseBase64Id)] = _database.DbBase64Id;
            initialRequest[nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _database.DbId.ToString();
            return initialRequest;
        }

        protected override void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            _database.NotificationCenter.Add(
                AlertRaised.Create(
                    _database.Name,
                    AlertTitle, msg, AlertType.Replication, NotificationSeverity.Warning, key: FromToString, details: new ExceptionDetails(e)));
        }

        private bool WaitForChanges(int timeout, CancellationToken token)
        {
            while (true)
            {
                using (var result = _interruptibleRead.ParseToMemory(
                    _waitForChanges,
                    "replication notify message",
                    timeout,
                    _buffer,
                    token))
                {
                    if (result.Document != null)
                    {
                        HandleServerResponse(result.Document, allowNotify: true);
                    }
                    else
                    {
                        return result.Timeout == false;
                    }
                }
            }
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (documentsContext.OpenReadTransaction())
            {
                if (_database.DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastEtagAccepted)
                {
                    // We have changes that the other side doesn't have, this can be because we have writes
                    // or because we have documents that were replicated to us. Either way, we need to sync
                    // those up with the remove side, so we'll start the replication loop again.
                    // We don't care if they are locally modified or not, because we filter documents that
                    // the other side already have (based on the change vector).
                    if ((DateTime.UtcNow - _lastDocumentSentTime).TotalMilliseconds > _parent.MinimalHeartbeatInterval)
                        _waitForChanges.Set();
                }
            }
        }

        protected override long GetLastHeartbeatTicks() => _parent.Database.Time.GetUtcNow().Ticks;

        private void OnDocumentChange(DocumentChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        private void OnCounterChange(CounterChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        private void OnTimeSeriesChange(TimeSeriesChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnChangeInternal(bool triggeredByReplicationThread)
        {
            if (triggeredByReplicationThread || ForTestingPurposes?.DisableWaitForChangesForExternalReplication == true)
                return;

            _waitForChanges.Set();
        }

        protected override void OnBeforeDispose()
        {
            _database.Changes.OnDocumentChange -= OnDocumentChange;
            _database.Changes.OnCounterChange -= OnCounterChange;
            _database.Changes.OnTimeSeriesChange -= OnTimeSeriesChange;
        }

        public override void Dispose()
        {
            base.Dispose();

            try
            {
                _waitForChanges.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //was already disposed? we don't care, we are disposing
            }
        }

        protected override void OnSuccessfulTwoWaysCommunication()
        {
            SuccessfulTwoWaysCommunication?.Invoke(this);
            MissingAttachmentsRetries = 0;
        }

        protected override void OnFailed(Exception e) => Failed?.Invoke(this, e);

        private void OnSuccessfulReplication() => SuccessfulReplication?.Invoke(this);

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            public Action OnDocumentSenderFetchNewItem;

            public Action<Dictionary<Slice, AttachmentReplicationItem>, SortedList<long, ReplicationBatchItem>> OnMissingAttachmentStream;

            public bool DisableWaitForChangesForExternalReplication;
        }
    }

    public interface IReportOutgoingReplicationPerformance
    {
        string DestinationFormatted { get; }

        OutgoingReplicationPerformanceStats[] GetReplicationPerformance();
    }

    public interface IReportIncomingReplicationPerformance
    {
        string DestinationFormatted { get; }

        IncomingReplicationPerformanceStats[] GetReplicationPerformance();
    }

    public static class ReplicationMessageType
    {
        public const string Heartbeat = "Heartbeat";
        public const string Documents = "Documents";
    }
}
