using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
using Raven.Client.Http.OAuth;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
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
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        public const string AlertTitle = "Replication";

        public event Action<OutgoingReplicationHandler> DocumentsSend;

        internal readonly DocumentDatabase _database;
        internal readonly string ServerNode;
        private readonly Logger _log;
        private readonly AsyncManualResetEvent _waitForChanges = new AsyncManualResetEvent();
        private readonly CancellationTokenSource _cts;
        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();
        private Thread _sendingThread;
        internal readonly ReplicationLoader _parent;
        internal long _lastSentDocumentEtag;
        public long LastAcceptedDocumentEtag;

        internal DateTime _lastDocumentSentTime;

        internal readonly Dictionary<Guid, long> _destinationLastKnownDocumentChangeVector =
            new Dictionary<Guid, long>();

        internal string _destinationLastKnownDocumentChangeVectorAsString;

        private TcpClient _tcpClient;

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        private JsonOperationContext.ManagedPinnedBuffer _buffer;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;
        private Stream _stream;
        private InterruptibleRead _interruptableRead;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;
        public readonly ReplicationNode Destination;

        private readonly ConcurrentQueue<OutgoingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<OutgoingReplicationStatsAggregator>();

        private OutgoingReplicationStatsAggregator _lastStats;

        public OutgoingReplicationHandler(ReplicationLoader parent,DocumentDatabase database,ReplicationNode node)
        {
            _parent = parent;
            _database = database;
            Destination = node;
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

        private string GetApiKey()
        {
            var watcher = Destination as DatabaseWatcher;
            return watcher == null ? _parent.GetClusterApiKey() : watcher.ApiKey;
        } 

        private void ReplicateToDestination()
        {
            NativeMemory.EnsureRegistered();
            try
            {
                var connectionInfo = ReplicationUtils.GetTcpInfo(Destination.Url, Destination.NodeTag, GetApiKey(), "Replication");

                if (_log.IsInfoEnabled)
                    _log.Info($"Will replicate to {Destination.NodeTag} @ {Destination.Url} via {connectionInfo.Url}");

                using (_tcpClient = new TcpClient())
                {
                    TcpUtils.ConnectSocketAsync(connectionInfo, _tcpClient, _log)
                        .Wait(CancellationToken);
                    var wrapSsl = TcpUtils.WrapStreamWithSslAsync(_tcpClient, connectionInfo);

                    wrapSsl
                        .Wait(CancellationToken);

                    using (_stream = wrapSsl.Result)
                    using (_interruptableRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream))
                    using (_buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        var documentSender = new ReplicationDocumentSender(_stream, this, _log);

                        WriteHeaderToRemotePeer();
                        //handle initial response to last etag and staff
                        try
                        {
                            var response = HandleServerResponse(getFullResponse: true);
                            if (response.Item1 == ReplicationMessageReply.ReplyType.Error)
                            {
                                if (response.Item2.Exception.Contains("DatabaseDoesNotExistException"))
                                    throw new DatabaseDoesNotExistException(response.Item2.Message,
                                        new InvalidOperationException(response.Item2.Exception));
                                throw new InvalidOperationException(response.Item2.Exception);
                            }
                        }
                        catch (DatabaseDoesNotExistException e)
                        {
                            var msg =
                                $"Failed to parse initial server replication response, because there is no database named {_database.Name} on the other end. " +
                                "In order for the replication to work, a database with the same name needs to be created at the destination";
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(msg, e);
                            }

                            AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }
                        catch (Exception e)
                        {
                            var msg =
                                $"Failed to parse initial server response. This is definitely not supposed to happen.";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);

                            AddAlertOnFailureToReachOtherSide(msg, e);

                            throw;
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            while (true)
                            {
                                var sp = Stopwatch.StartNew();
                                var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                                AddReplicationPerformance(stats);

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
                                        catch (Exception e)
                                        {
                                            scope.AddError(e);
                                            throw;
                                        }
                                    }
                                }
                                finally
                                {
                                    stats.Complete();
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (WaitForChanges(_parent.MinimalHeartbeatInterval, _cts.Token) == false)
                            {
                                SendHeartbeat();
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
                        $"Unexpected exception occured on replication thread ({FromToString}). Replication stopped (will be retried later).",
                        e);
                Failed?.Invoke(this, e);
            }
        }

        private void AddReplicationPerformance(OutgoingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out stats);
        }

        private void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
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
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                 //send initial connection information               
                var token = AsyncHelpers.RunSync(() => _authenticator.GetAuthenticationTokenAsync(GetApiKey(), Destination.Url, documentsContext));

                documentsContext.Write(writer, new DynamicJsonValue
                {
                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = Destination.Database,// _parent.Database.Name,
                    [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                    [nameof(TcpConnectionHeaderMessage.AuthorizationToken)] = token,
                    [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = _parent._server.NodeTag,
                });
                writer.Flush();
                ReadHeaderResponseAndThrowIfUnAuthorized();
                //start request/response for fetching last etag
                var request = new DynamicJsonValue
                {
                    ["Type"] = "GetLastEtag",
                    [nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _database.DbId.ToString(),
                    [nameof(ReplicationLatestEtagRequest.SourceDatabaseName)] = _database.Name,
                    [nameof(ReplicationLatestEtagRequest.SourceUrl)] = _database.Configuration.Core.ServerUrl,
                    [nameof(ReplicationLatestEtagRequest.SourceTag)] = _parent._server.NodeTag,
                    [nameof(ReplicationLatestEtagRequest.SourceMachineName)] = Environment.MachineName,
                };

                documentsContext.Write(writer, request);
                writer.Flush();
            }
        }

        private void ReadHeaderResponseAndThrowIfUnAuthorized()
        {
            var timeout = 2 * 60 * 1000; // TODO: configurable
            using (var replicationTcpConnectReplyMessage = _interruptableRead.ParseToMemory(
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
                    case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                        //All good nothing to do
                        break;
                    default:
                        throw new UnauthorizedAccessException($"{Destination.Url}/{Destination.NodeTag} replied with failure {headerResponse.Status}");
                }
            }
        }

        private bool WaitForChanges(int timeout, CancellationToken token)
        {
            while (true)
            {
                using (var result = _interruptableRead.ParseToMemory(
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
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
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

            _destinationLastKnownDocumentChangeVector.Clear();

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        private void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = Math.Max(_lastSentDocumentEtag, replicationBatchReply.LastEtagAccepted);
            LastAcceptedDocumentEtag = replicationBatchReply.LastEtagAccepted;

            _destinationLastKnownDocumentChangeVectorAsString = replicationBatchReply.DocumentsChangeVector.Format();

            foreach (var changeVectorEntry in replicationBatchReply.DocumentsChangeVector)
            {
                _destinationLastKnownDocumentChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }

            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
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
                        _waitForChanges.SetByAsyncCompletion();
                }
            }
        }

        public string FromToString => $"from {_database.Name} at {_parent._server.NodeTag} to {Destination.NodeTag}({Destination.Database}) at {Destination.Url}";

        public ReplicationNode Node => Destination;
        public string DestinationFormatted => $"{Destination.Url}/databases/{Destination.Database}";

        internal void SendHeartbeat()
        {
            DocumentsOperationContext documentsContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
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
                    documentsContext.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                    throw;
                }

                try
                {
                    HandleServerResponse();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Parsing heartbeat result failed. ({FromToString})", e);
                    throw;
                }
            }
        }

        internal Tuple<ReplicationMessageReply.ReplyType, ReplicationMessageReply> HandleServerResponse(bool getFullResponse = false)
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000;// TODO: configurable
                if (Debugger.IsAttached)
                    timeout *= 10;
                using (var replicationBatchReplyMessage = _interruptableRead.ParseToMemory(
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

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage.Document,
                        allowNotify: false);
                    if (replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

                    var sendFullReply = replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error ||
                                        getFullResponse;

                    return Tuple.Create(replicationBatchReply.Type, sendFullReply ? replicationBatchReply : null);
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

        internal ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage,
            bool allowNotify)
        {
            replicationBatchReplyMessage.BlittableValidation();
            var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);
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
                    var msg =
                        $"Received error from remote replication destination. Error received: {replicationBatchReply.Exception}";
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(msg);
                    }
                    break;
            }

            if (_log.IsInfoEnabled)
            {
                switch (replicationBatchReply.Type)
                {
                    case ReplicationMessageReply.ReplyType.Ok:
                        _log.Info(
                            $"Received reply for replication batch from {Destination.NodeTag} @ {Destination.Url}. New destination change vector is {_destinationLastKnownDocumentChangeVectorAsString}");
                        break;
                    case ReplicationMessageReply.ReplyType.Error:
                        _log.Info(
                            $"Received reply for replication batch from {Destination.NodeTag} at {Destination.Url}. There has been a failure, error string received : {replicationBatchReply.Exception}");
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
      
        private int _disposed;
        public void Dispose()
        {
            //There are multiple invokations of dispose, this happens sometimes during tests, causing failures.
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 1)
                return;
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString})");

            _database.Changes.OnDocumentChange -= OnDocumentChange;

            _cts.Cancel();

            try
            {
                _tcpClient?.Dispose();
            }
            catch (Exception) { }

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
        public const string IndexesTransformers = "IndexesTransformers";
        public const string Documents = "Documents";
    }
}