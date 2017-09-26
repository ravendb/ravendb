using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        public const string AlertTitle = "Replication";

        public event Action<OutgoingReplicationHandler> DocumentsSend;
        public event Action<LiveReplicationPulsesCollector.ReplicationPulse> HandleReplicationPulse;

        internal readonly DocumentDatabase _database;
        private readonly Logger _log;
        private readonly AsyncManualResetEvent _waitForChanges = new AsyncManualResetEvent();
        private readonly CancellationTokenSource _cts;
        private Thread _sendingThread;
        internal readonly ReplicationLoader _parent;
        internal long _lastSentDocumentEtag;

        internal DateTime _lastDocumentSentTime;

        internal string LastAcceptedChangeVector;

        internal string _destinationLastKnownChangeVectorAsString;

        private TcpClient _tcpClient;

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        private JsonOperationContext.ManagedPinnedBuffer _buffer;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;
        private Stream _stream;
        private InterruptibleRead _interruptibleRead;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;
        public readonly ReplicationNode Destination;
        private readonly bool _external;

        private readonly ConcurrentQueue<OutgoingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<OutgoingReplicationStatsAggregator>();
        private OutgoingReplicationStatsAggregator _lastStats;
        public TcpConnectionInfo ConnectionInfo;

        public OutgoingReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, bool external)
        {
            _parent = parent;
            _database = database;
            Destination = node;
            _external = external;
            _log = LoggingSource.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);

            _database.Changes.OnDocumentChange += OnDocumentChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public OutgoingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            var lastStats = _lastStats;

            return _lastReplicationStats
                .Select(x => x == lastStats ? x.ToReplicationPerformanceLiveStatsWithDetails() : x.ToReplicationPerformanceStats())
                .ToArray();
        }

        public OutgoingReplicationStatsAggregator GetLatestReplicationPerformance()
        {
            return _lastStats;
        }

        public void Start()
        {
            _sendingThread = new Thread(ReplicateToDestination)
            {
                Name = OutgoingReplicationThreadName,
                IsBackground = true
            };
            _sendingThread.Start();
        }

        public string OutgoingReplicationThreadName => $"Outgoing replication {FromToString}";

        public string GetNode()
        {
            var node = Destination as InternalReplication;
            return node?.NodeTag;
        }

        private void ReplicateToDestination()
        {
            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
            NativeMemory.EnsureRegistered();
            try
            {
                if (ConnectionInfo == null)
                {
                    ConnectionInfo = ReplicationUtils.GetTcpInfo(Destination.Url, GetNode(), "Replication",
                        _parent._server.Server.ClusterCertificateHolder.Certificate);
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"Will replicate to {Destination.FromString()} via {ConnectionInfo.Url}");

                using (_parent._server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var record = _parent.LoadDatabaseRecord();
                    if (record == null)
                        throw new InvalidOperationException($"The database record for {_parent.Database.Name} does not exist?!");

                    if (record.Encrypted && Destination.Url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                        throw new InvalidOperationException(
                            $"{record.DatabaseName} is encrypted, and require HTTPS for replication, but had endpoint with url {Destination.Url} to database {Destination.Database}");
                }

                var task = TcpUtils.ConnectSocketAsync(ConnectionInfo, _parent._server.Engine.TcpConnectionTimeout, _log);
                task.Wait(CancellationToken);
                using (_tcpClient = task.Result)
                {
                    var wrapSsl = TcpUtils.WrapStreamWithSslAsync(_tcpClient, ConnectionInfo, _parent._server.Server.ClusterCertificateHolder.Certificate);

                    wrapSsl.Wait(CancellationToken);

                    using (_stream = wrapSsl.Result)
                    using (_interruptibleRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream))
                    using (_buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        var documentSender = new ReplicationDocumentSender(_stream, this, _log);

                        WriteHeaderToRemotePeer();
                        //handle initial response to last etag and staff
                        try
                        {
                            var response = HandleServerResponse(getFullResponse: true);
                            if (response.ReplyType == ReplicationMessageReply.ReplyType.Error)
                            {
                                var exception = new InvalidOperationException(response.Reply.Exception);
                                if (response.Reply.Exception.Contains(nameof(DatabaseDoesNotExistException)) ||
                                    response.Reply.Exception.Contains(nameof(DatabaseNotRelevantException)))
                                {
                                    AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, "Database does not exist");
                                    DatabaseDoesNotExistException.ThrowWithMessageAndException(Destination.Database, response.Reply.Message, exception);
                                }

                                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, $"Got error: {response.Reply.Exception}");
                                throw exception;
                            }
                        }
                        catch (DatabaseDoesNotExistException e)
                        {
                            var msg = $"Failed to parse initial server replication response, because there is no database named {_database.Name} " +
                                      "on the other end. ";
                            if (_external)
                                msg += "In order for the replication to work, a database with the same name needs to be created at the destination";

                            var young = (DateTime.UtcNow - _startedAt).TotalSeconds < 30;
                            if (young)
                                msg += "This can happen if the other node wasn't yet notified about being assigned this database and should be resolved shortly.";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);

                            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);

                            // won't add an alert on young connections
                            // because it may take a few seconds for the other side to be notified by
                            // the cluster that it has this db.
                            if (young == false)
                                AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }
                        catch (OperationCanceledException e)
                        {
                            const string msg = "Got operation canceled notification while opening outgoing replication channel. " +
                                               "Aborting and closing the channel.";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);
                            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                            throw;
                        }
                        catch (Exception e)
                        {
                            const string msg = "Failed to parse initial server response. This is definitely not supposed to happen.";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);

                            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                            AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            while (true)
                            {
                                if (_parent.DebugWaitAndRunReplicationOnce != null)
                                {
                                    _parent.DebugWaitAndRunReplicationOnce.Wait(_cts.Token);
                                    _parent.DebugWaitAndRunReplicationOnce.Reset();
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
                                            var didWork = documentSender.ExecuteReplicationOnce(scope);
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
                                            AddReplicationPulse(ReplicationPulseDirection.OutgoingError, e.Message);

                                            scope.AddError(e);
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

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (WaitForChanges(_parent.MinimalHeartbeatInterval, _cts.Token) == false)
                            {
                                // open tx
                                // read current change vector compare to last sent
                                // if okay, send cv
                                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                                using (var tx = ctx.OpenReadTransaction())
                                {
                                    var etag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                                    if (etag == _lastSentDocumentEtag)
                                    {
                                        SendHeartbeat(DocumentsStorage.GetDatabaseChangeVector(ctx));
                                    }
                                    else
                                    {
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
                }
            }
            catch (OperationCanceledException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Operation canceled on replication thread ({FromToString}). This is not necessary due to an issue. Stopped the thread.");
                Failed?.Invoke(this, e);
            }
            catch (IOException e)
            {
                if (_log.IsInfoEnabled)
                {
                    if (e.InnerException is SocketException)
                        _log.Info(
                            $"SocketException was thrown from the connection to remote node ({FromToString}). This might mean that the remote node is done or there is a network issue.",
                            e);
                    else
                        _log.Info($"IOException was thrown from the connection to remote node ({FromToString}).", e);
                }
                Failed?.Invoke(this, e);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Unexpected exception occurred on replication thread ({FromToString}). Replication stopped (will be retried later).",
                        e);
                Failed?.Invoke(this, e);
            }
        }

        private void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null)
        {
            HandleReplicationPulse?.Invoke(new LiveReplicationPulsesCollector.ReplicationPulse
            {
                OccurredAt = SystemTime.UtcNow,
                Direction = direction,
                To = Destination,
                IsExternal = _external,
                ExceptionMessage = exceptionMessage
            });
        }

        private void AddReplicationPerformance(OutgoingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out _);
        }

        private void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationContext))
            using (var txw = configurationContext.OpenWriteTransaction())
            {
                _database.NotificationCenter.AddAfterTransactionCommit(
                    AlertRaised.Create(AlertTitle, msg, AlertType.Replication, NotificationSeverity.Warning, key: FromToString, details: new ExceptionDetails(e)),
                    txw);

                txw.Commit();
            }
        }

        private void WriteHeaderToRemotePeer()
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, new DynamicJsonValue
                {
                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = Destination.Database,// _parent.Database.Name,
                    [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                    [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = _parent._server.NodeTag,
                    [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.ReplicationTcpVersion
                });
                writer.Flush();
                ReadHeaderResponseAndThrowIfUnAuthorized();
                //start request/response for fetching last etag
                var request = new DynamicJsonValue
                {
                    ["Type"] = "GetLastEtag",
                    [nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _database.DbId.ToString(),
                    [nameof(ReplicationLatestEtagRequest.SourceDatabaseName)] = _database.Name,
                    [nameof(ReplicationLatestEtagRequest.SourceUrl)] = _parent._server.NodeHttpServerUrl,
                    [nameof(ReplicationLatestEtagRequest.SourceTag)] = _parent._server.NodeTag,
                    [nameof(ReplicationLatestEtagRequest.SourceMachineName)] = Environment.MachineName
                };

                documentsContext.Write(writer, request);
                writer.Flush();
            }
        }

        private void ReadHeaderResponseAndThrowIfUnAuthorized()
        {
            const int timeout = 2 * 60 * 1000; // TODO: configurable
            using (var replicationTcpConnectReplyMessage = _interruptibleRead.ParseToMemory(
                _connectionDisposed,
                "replication acknowledge response",
                timeout,
                _buffer,
                CancellationToken))
            {
                if (replicationTcpConnectReplyMessage.Timeout)
                {
                    ThrowTimeout(timeout);
                }
                if (replicationTcpConnectReplyMessage.Interrupted)
                {
                    ThrowConnectionClosed();
                }
                var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(replicationTcpConnectReplyMessage.Document);
                switch (headerResponse.Status)
                {
                    case TcpConnectionStatus.Ok:
                        break;
                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new UnauthorizedAccessException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                    case TcpConnectionStatus.TcpVersionMismatch:
                        throw new InvalidOperationException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                }
            }
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

        internal void WriteToServer(DynamicJsonValue val)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, val);
            }
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            if (replicationBatchReply.MessageType == null)
                throw new InvalidOperationException(
                    "MessageType on replication response is null. This is likely is a symptom of an issue, and should be investigated.");

            LastAcceptedChangeVector = null;

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        private class UpdateSiblingCurrentEtag : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly ReplicationMessageReply _replicationBatchReply;
            private readonly AsyncManualResetEvent _trigger;
            private Guid _dbId;

            public UpdateSiblingCurrentEtag(ReplicationMessageReply replicationBatchReply, AsyncManualResetEvent trigger)
            {
                _replicationBatchReply = replicationBatchReply;
                _trigger = trigger;
            }

            public bool InitAndValidate()
            {
                if (Guid.TryParse(_replicationBatchReply.DatabaseId, out _dbId) == false)
                    return false;

                return _replicationBatchReply.CurrentEtag > 0;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                if (string.IsNullOrEmpty(context.LastDatabaseChangeVector))
                    context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                var status = ChangeVectorUtils.GetConflictStatus(_replicationBatchReply.DatabaseChangeVector,
                    context.LastDatabaseChangeVector);

                if (status != ConflictStatus.AlreadyMerged)
                    return 0;

                var res = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, ref context.LastDatabaseChangeVector) ? 1 : 0;
                if (res == 1)
                {
                    context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                    {
                        try
                        {
                            _trigger.Set();
                        }
                        catch
                        {
                            //
                        }
                    };
                }
                return res;
            }
        }

        private void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = Math.Max(_lastSentDocumentEtag, replicationBatchReply.LastEtagAccepted);

            LastAcceptedChangeVector = replicationBatchReply.DatabaseChangeVector;
            if (_external == false)
            {
                var update = new UpdateSiblingCurrentEtag(replicationBatchReply, _waitForChanges);
                if (update.InitAndValidate())
                {
                    // we intentionally not waiting here, there is nothing that depends on the timing on this, since this 
                    // is purely advisory. We just want to have the information up to date at some point, and we won't 
                    // miss anything much if this isn't there.
                    _database.TxMerger.Enqueue(update).AsTask().IgnoreUnobservedExceptions();
                }
            }

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (documentsContext.OpenReadTransaction())
            {
                if (DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction) !=
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

        public string FromToString => $"from {_database.Name} at {_parent._server.NodeTag} to {Destination.FromString()}";

        public ReplicationNode Node => Destination;
        public string DestinationFormatted => $"{Destination.Url}/databases/{Destination.Database}";

        internal void SendHeartbeat(string changeVector)
        {
            AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeat);

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                try
                {
                    var heartbeat = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastSentDocumentEtag,
                        [nameof(ReplicationMessageHeader.ItemsCount)] = 0
                    };
                    if (changeVector != null)
                    {
                        heartbeat[nameof(ReplicationMessageHeader.DatabaseChangeVector)] = changeVector;
                    }
                    documentsContext.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatError, "Sending heartbeat failed.");
                    throw;
                }

                try
                {
                    HandleServerResponse();
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledge);
                }
                catch (OperationCanceledException)
                {
                    const string msg = "Got cancellation notification while parsing heartbeat response. Closing replication channel.";
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})");
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
                catch (Exception e)
                {
                    const string msg = "Parsing heartbeat result failed.";
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})", e);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
            }
        }

        internal (ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) HandleServerResponse(bool getFullResponse = false)
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000; // TODO: configurable
                DebuggerAttachedTimeout.OutgoingReplication(ref timeout);

                using (var replicationBatchReplyMessage = _interruptibleRead.ParseToMemory(
                    _connectionDisposed,
                    "replication acknowledge message",
                    timeout,
                    _buffer,
                    CancellationToken))
                {
                    if (replicationBatchReplyMessage.Timeout)
                    {
                        ThrowTimeout(timeout);
                    }
                    if (replicationBatchReplyMessage.Interrupted)
                    {
                        ThrowConnectionClosed();
                    }

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage.Document, allowNotify: false);
                    if (replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

                    var sendFullReply = replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error ||
                                        getFullResponse;

                    var type = replicationBatchReply.Type;
                    var reply = sendFullReply ? replicationBatchReply : null;
                    return (type, reply);
                }
            }
        }

        private static void ThrowTimeout(int timeout)
        {
            throw new TimeoutException("Could not get a server response in a reasonable time " +
                                       TimeSpan.FromMilliseconds(timeout));
        }

        private static void ThrowConnectionClosed()
        {
            throw new OperationCanceledException("The connection has been closed by the Dispose method");
        }

        internal ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage, bool allowNotify)
        {
            replicationBatchReplyMessage.BlittableValidation();
            var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);

            if (replicationBatchReply.MessageType == "Processing")
                return null;

            if (allowNotify == false && replicationBatchReply.MessageType == "Notify")
                return null;

            DestinationDbId = replicationBatchReply.DatabaseId;

            switch (replicationBatchReply.Type)
            {
                case ReplicationMessageReply.ReplyType.Ok:
                    UpdateDestinationChangeVector(replicationBatchReply);
                    OnSuccessfulTwoWaysCommunication();
                    break;
                default:
                    var msg = $"Received error from remote replication destination. Error received: {replicationBatchReply.Exception}";
                    if (_log.IsInfoEnabled)
                        _log.Info(msg);
                    break;
            }

            if (_log.IsInfoEnabled)
            {
                switch (replicationBatchReply.Type)
                {
                    case ReplicationMessageReply.ReplyType.Ok:
                        _log.Info(
                            $"Received reply for replication batch from {Destination.FromString()}. New destination change vector is {_destinationLastKnownChangeVectorAsString}");
                        break;
                    case ReplicationMessageReply.ReplyType.Error:
                        _log.Info(
                            $"Received reply for replication batch from {Destination.FromString()}. There has been a failure, error string received : {replicationBatchReply.Exception}");
                        throw new InvalidOperationException(
                            $"Received failure reply for replication batch. Error string received = {replicationBatchReply.Exception}");
                    default:
                        throw new ArgumentOutOfRangeException(nameof(replicationBatchReply),
                            "Received reply for replication batch with unrecognized type... got " +
                            replicationBatchReply.Type);
                }
            }
            return replicationBatchReply;
        }


        private void OnDocumentChange(DocumentChange change)
        {
            if (change.TriggeredByReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly DateTime _startedAt = DateTime.UtcNow;

        public void Dispose()
        {
            // There are multiple invocations of dispose, this happens sometimes during tests, causing failures.
            if (!_disposed.Raise())
                return;
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString})");

            _database.Changes.OnDocumentChange -= OnDocumentChange;

            _cts.Cancel();

            try
            {
                _tcpClient?.Dispose();
            }
            catch (Exception)
            {
                // nothing we can do here
            }

            _connectionDisposed.Set();

            if (_sendingThread != Thread.CurrentThread)
            {
                _sendingThread?.Join();
            }

            try
            {
                _cts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //was already disposed? we don't care, we are disposing
            }
        }

        private void OnSuccessfulTwoWaysCommunication() => SuccessfulTwoWaysCommunication?.Invoke(this);
    }

    public static class ReplicationMessageType
    {
        public const string Heartbeat = "Heartbeat";
        public const string Documents = "Documents";
    }
}
