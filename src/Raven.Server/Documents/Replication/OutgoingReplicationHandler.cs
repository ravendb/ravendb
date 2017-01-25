using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Raven.Server.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.NewClient.Client.Exceptions.Database;
using Raven.Server.Alerts;
using Raven.Server.Extensions;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        internal readonly DocumentDatabase _database;
        internal readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly AsyncManualResetEvent _waitForChanges = new AsyncManualResetEvent();
        private readonly CancellationTokenSource _cts;
        private Thread _sendingThread;
        internal readonly DocumentReplicationLoader _parent;
        internal long _lastSentDocumentEtag;
        public long LastAcceptedDocumentEtag;
        internal long _lastSentIndexOrTransformerEtag;

        internal DateTime _lastDocumentSentTime;
        internal DateTime _lastIndexOrTransformerSentTime;

        internal readonly Dictionary<Guid, long> _destinationLastKnownDocumentChangeVector =
            new Dictionary<Guid, long>();

        internal readonly Dictionary<Guid, long> _destinationLastKnownIndexOrTransformerChangeVector =
            new Dictionary<Guid, long>();

        internal string _destinationLastKnownDocumentChangeVectorAsString;
        internal string _destinationLastKnownIndexOrTransformerChangeVectorAsString;

        private TcpClient _tcpClient;

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        private JsonOperationContext.ManagedPinnedBuffer _buffer;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;
        private NetworkStream _stream;
        private InterruptibleRead _interruptableRead;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

        public OutgoingReplicationHandler(DocumentReplicationLoader parent,
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _parent = parent;
            _database = database;
            _destination = destination;
            _log = LoggingSource.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _database.Notifications.OnDocumentChange += OnDocumentChange;
            _database.Notifications.OnIndexChange += OnIndexChange;
            _database.Notifications.OnTransformerChange += OnTransformerChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _sendingThread = new Thread(ReplicateToDestination)
            {
                Name = $"Outgoing replication {FromToString}",
                IsBackground = true
            };
            _sendingThread.Start();
        }

        private TcpConnectionInfo GetTcpInfo()
        {
            var convention = new DocumentConvention();
            //since we use it only once when the connection is initialized, no reason to keep requestFactory around for long
            using (var requestFactory = new HttpJsonRequestFactory(1))
            using (
                var request =
                    requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                        string.Format("{0}/info/tcp",
                            MultiDatabase.GetRootDatabaseUrl(_destination.Url)),
                        HttpMethod.Get,
                        new OperationCredentials(_destination.ApiKey, CredentialCache.DefaultCredentials), convention)
                    {
                        Timeout = TimeSpan.FromSeconds(15)
                    }))
            {

                var result = request.ReadResponseJson();
                var tcpConnectionInfo =
                    convention.CreateSerializer().Deserialize<TcpConnectionInfo>(new RavenJTokenReader(result));
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Will replicate to {_destination.Database} @ {_destination.Url} via {tcpConnectionInfo.Url}");
                }
                return tcpConnectionInfo;
            }
        }

        private void ReplicateToDestination()
        {
            try
            {
                var connectionInfo = GetTcpInfo();

                using (_tcpClient = new TcpClient())
                {
                    ConnectSocket(connectionInfo, _tcpClient);

                    using (_stream = _tcpClient.GetStream())
                    using (_interruptableRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream))
                    using (_buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        var documentSender = new ReplicationDocumentSender(_stream, this, _log);
                        var indexAndTransformerSender = new ReplicationIndexTransformerSender(_stream, this, _log);

                        WriteHeaderToRemotePeer();

                        //handle initial response to last etag and staff
                        try
                        {
                            var response = HandleServerResponse(serveFullResponse: true);
                            if (response.Item1 == ReplicationMessageReply.ReplyType.Error)
                            {
                                if (response.Item2.Exception.Contains("DatabaseDoesNotExistException"))
                                    throw new DatabaseDoesNotExistException(response.Item2.Message,
                                        new InvalidOperationException(response.Item2.Exception));
                                throw new InvalidOperationException(response.Item2.Exception);
                            }
                            if (response.Item1 == ReplicationMessageReply.ReplyType.Ok)
                                    {
                                        if (response.Item2?.ResolverId != null)
                                        {
                                            _parent.ResolverLeader.ParseAndUpdate(response.Item2.ResolverId, response.Item2.ResolverVersion);
                                        }
                                    }
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

                            AddAlertOnFailureToReachOtherSide(msg);

                            throw;
                        }
                        catch (Exception e)
                        {
                            var msg =
                                $"Failed to parse initial server response. This is definitely not supposed to happen. Exception thrown: {e}";
                            if (_log.IsInfoEnabled)
                                _log.Info(msg, e);

                            AddAlertOnFailureToReachOtherSide(msg);

                            throw;
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            long currentEtag;

                            Debug.Assert(_database.IndexMetadataPersistence.IsInitialized);

                            currentEtag = GetLastIndexEtag();

                            if (_destination.SkipIndexReplication == false &&
                                currentEtag != indexAndTransformerSender.LastEtag)
                            {
                                indexAndTransformerSender.ExecuteReplicationOnce();
                            }

                            var sp = Stopwatch.StartNew();
                            while (documentSender.ExecuteReplicationOnce())
                            {
                                if (sp.ElapsedMilliseconds > 60 * 1000)
                                {
                                    _waitForChanges.Set();
                                    break;
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (WaitForChanges(_parent._minimalHeartbeatInterval, _cts.Token) == false)
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
                    _log.Info($"Operation canceled on replication thread ({FromToString}). Stopped the thread.");
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

        private long GetLastIndexEtag()
        {
            long currentEtag;
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (configurationContext.OpenReadTransaction())
                currentEtag =
                    _database.IndexMetadataPersistence.ReadLastEtag(configurationContext.Transaction.InnerTransaction);
            return currentEtag;
        }

        private void AddAlertOnFailureToReachOtherSide(string msg)
        {
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (var txw = configurationContext.OpenWriteTransaction())
            {
                _database.Alerts.AddAlert(new Alert
                {
                    Key = FromToString,
                    Type = AlertType.Replication,
                    Message = msg,
                    CreatedAt = DateTime.UtcNow,
                    Severity = AlertSeverity.Warning
                }, configurationContext, txw);
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
                documentsContext.Write(writer, new DynamicJsonValue
                {
                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _destination.Database,
                    [nameof(TcpConnectionHeaderMessage.Operation)] =
                    TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                });

              //start request/response for fetching last etag
              var request = new DynamicJsonValue
                            {
                                ["Type"] = "GetLastEtag",
                                ["SourceDatabaseId"] = _database.DbId.ToString(),
                                ["SourceDatabaseName"] = _database.Name,
                                ["SourceUrl"] = _database.Configuration.Core.ServerUrl,
                                ["MachineName"] = Environment.MachineName,
                            };
                            if (_parent.ResolverLeader.HasLeader())
                            {
                                request["ResolverVersion"] = _parent.ResolverLeader.Version.ToString();
                                request["ResolverId"] = _parent.ResolverLeader.Dbid.ToString();
                            }
              
                
                documentsContext.Write(writer, request);
                writer.Flush();
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
            _lastSentIndexOrTransformerEtag = Math.Max(_lastSentIndexOrTransformerEtag,
                replicationBatchReply.LastIndexTransformerEtagAccepted);
            LastAcceptedDocumentEtag = replicationBatchReply.LastEtagAccepted;

            _destinationLastKnownDocumentChangeVectorAsString = replicationBatchReply.DocumentsChangeVector.Format();
            _destinationLastKnownIndexOrTransformerChangeVectorAsString =
                replicationBatchReply.IndexTransformerChangeVector.Format();

            foreach (var changeVectorEntry in replicationBatchReply.DocumentsChangeVector)
            {
                _destinationLastKnownDocumentChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }

            foreach (var changeVectorEntry in replicationBatchReply.IndexTransformerChangeVector)
            {
                _destinationLastKnownIndexOrTransformerChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
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
                    if ((DateTime.UtcNow - _lastDocumentSentTime).TotalMilliseconds > _parent._minimalHeartbeatInterval)
                        _waitForChanges.SetByAsyncCompletion();
                }
            }
            TransactionOperationContext configurationContext;
            using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
            using (configurationContext.OpenReadTransaction())
            {
                if (
                    _database.IndexMetadataPersistence.ReadLastEtag(configurationContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastIndexTransformerEtagAccepted)
                {
                    if ((DateTime.UtcNow - _lastIndexOrTransformerSentTime).TotalMilliseconds >
                        _parent._minimalHeartbeatInterval)
                        _waitForChanges.SetByAsyncCompletion();
                }
            }
        }

        private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}"
            ;

        public ReplicationDestination Destination => _destination;

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
                    [nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag)] = _lastSentIndexOrTransformerEtag,
                    [nameof(ReplicationMessageHeader.ItemCount)] = 0
                };
                if (_parent.ResolverLeader.HasLeader())
                {
                    heartbeat[nameof(ReplicationMessageHeader.ResovlerVersion)] = _parent.ResolverLeader.Version.ToString();
                    heartbeat[nameof(ReplicationMessageHeader.ResovlerId)] = _parent.ResolverLeader.Dbid.ToString();
                }
                _documentsContext.Write(_writer, heartbeat);
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
                var response = HandleServerResponse(serveFullResponse:true);
                if (response.Item2?.ResolverVersion != null)
                {
                    _parent.ResolverLeader.ParseAndUpdate(response.Item2.ResolverId, response.Item2.ResolverVersion);
                }
            }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Parsing heartbeat result failed. ({FromToString})", e);
                    throw;
                }
            }
        }

        internal Tuple<ReplicationMessageReply.ReplyType, ReplicationMessageReply> HandleServerResponse()
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000;// TODO: configurable
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
                                        serveFullResponse;

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
                            $"Received reply for replication batch from {_destination.Database} @ {_destination.Url}. New destination change vector is {_destinationLastKnownDocumentChangeVectorAsString}");
                        break;
                    case ReplicationMessageReply.ReplyType.Error:
                        _log.Info(
                            $"Received reply for replication batch from {_destination.Database} at {_destination.Url}. There has been a failure, error string received : {replicationBatchReply.Exception}");
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

        private void ConnectSocket(TcpConnectionInfo connection, TcpClient tcpClient)
        {
            var uri = new Uri(connection.Url);
            var host = uri.Host;
            var port = uri.Port;
            try
            {
                tcpClient.ConnectAsync(host, port).Wait(CancellationToken);
            }
            catch (SocketException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Failed to connect to remote replication destination {connection.Url}. Socket Error Code = {e.SocketErrorCode}",
                        e);
                throw;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {connection.Url}", e);
                throw;
            }
        }

        private void OnDocumentChange(DocumentChangeNotification notification)
        {
            if (notification.TriggeredByReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private void OnIndexChange(IndexChangeNotification notification)
        {
            if (notification.Type != IndexChangeTypes.IndexAdded &&
                notification.Type != IndexChangeTypes.IndexRemoved)
                return;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Received index {notification.Type} event, index name = {notification.Name}, etag = {notification.Etag}");

            if (IncomingReplicationHandler.IsIncomingReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private void OnTransformerChange(TransformerChangeNotification notification)
        {
            if (notification.Type != TransformerChangeTypes.TransformerAdded &&
                notification.Type != TransformerChangeTypes.TransformerRemoved)
                return;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Received transformer {notification.Type} event, transformer name = {notification.Name}, etag = {notification.Etag}");

            if (IncomingReplicationHandler.IsIncomingReplicationThread)
                return;
            _waitForChanges.Set();
        }

        public void Dispose()
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString})");

            _database.Notifications.OnDocumentChange -= OnDocumentChange;
            _database.Notifications.OnIndexChange -= OnIndexChange;
            _database.Notifications.OnTransformerChange -= OnTransformerChange;

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