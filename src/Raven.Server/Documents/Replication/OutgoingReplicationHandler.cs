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
using Raven.Server.Alerts;
using Raven.Server.Exceptions;
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
        private readonly int _minimalHeartbeatInterval = 15 * 1000;// ms - 15 seconds
        private Thread _sendingThread;

        internal long _lastSentDocumentEtag;
        public long LastAcceptedDocumentEtag;
        internal long _lastSentIndexOrTransformerEtag;

        internal DateTime _lastDocumentSentTime;
        internal DateTime _lastIndexOrTransformerSentTime;

        internal readonly Dictionary<Guid, long> _destinationLastKnownDocumentChangeVector = new Dictionary<Guid, long>();
        internal readonly Dictionary<Guid, long> _destinationLastKnownIndexOrTransformerChangeVector = new Dictionary<Guid, long>();

        internal string _destinationLastKnownDocumentChangeVectorAsString;
        internal string _destinationLastKnownIndexOrTransformerChangeVectorAsString;

        private TcpClient _tcpClient;
        private BlittableJsonTextWriter _writer;
        private JsonOperationContext.MultiDocumentParser _parser;
        internal DocumentsOperationContext _documentsContext;
        internal TransactionOperationContext _configurationContext;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

        public event Action<OutgoingReplicationHandler> Shutdown;

        public OutgoingReplicationHandler(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
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
            using (var request = requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, string.Format("{0}/info/tcp",
                MultiDatabase.GetRootDatabaseUrl(_destination.Url)),
                HttpMethod.Get,
                new OperationCredentials(_destination.ApiKey, CredentialCache.DefaultCredentials), convention)
            {
                Timeout = TimeSpan.FromSeconds(15)
            }))
            {

                var result = request.ReadResponseJson();
                var tcpConnectionInfo = convention.CreateSerializer().Deserialize<TcpConnectionInfo>(new RavenJTokenReader(result));
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Will replicate to {_destination.Database} @ {_destination.Url} via {tcpConnectionInfo.Url}");
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

                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out _documentsContext))
                    using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out _configurationContext))
                    using (var stream = _tcpClient.GetStream())
                    {
                        var documentSender = new ReplicationDocumentSender(stream, this, _log);
                        var indexAndTransformerSender = new ReplicationIndexTransformerSender(stream, this, _log);

                        using (_writer = new BlittableJsonTextWriter(_documentsContext, stream))
                        using (_parser = _documentsContext.ParseMultiFrom(stream))
                        {
                            //send initial connection information
                            _documentsContext.Write(_writer, new DynamicJsonValue
                            {
                                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _destination.Database,
                                [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                            });

                            //start request/response for fetching last etag
                            _documentsContext.Write(_writer, new DynamicJsonValue
                            {
                                ["Type"] = "GetLastEtag",
                                ["SourceDatabaseId"] = _database.DbId.ToString(),
                                ["SourceDatabaseName"] = _database.Name,
                                ["SourceUrl"] = _database.Configuration.Core.ServerUrl,
                                ["MachineName"] = Environment.MachineName,
                            });
                            _writer.Flush();

                            //handle initial response to last etag and staff
                            try
                            {
                                using (_configurationContext.OpenReadTransaction())
                                using (_documentsContext.OpenReadTransaction())
                                {
                                    var response = HandleServerResponse();
                                    if (response.Item1 == ReplicationMessageReply.ReplyType.Error)
                                    {
                                        if(response.Item2.Exception.Contains("DatabaseDoesNotExistsException"))
                                            throw new DatabaseDoesNotExistsException(response.Item2.Message, new InvalidOperationException(response.Item2.Exception));

                                        throw new InvalidOperationException(response.Item2.Exception);
                                    }                                    
                                }
                            }
                            catch (DatabaseDoesNotExistsException e)
                            {
                                var msg = $"Failed to parse initial server replication response, because there is no database named {_database.Name} on the other end. " +
                                          "In order for the replication to work, a database with the same name needs to be created at the destination";
                                if (_log.IsInfoEnabled)
                                {
                                    _log.Info(msg,e);
                                }

                                using (var txw = _configurationContext.OpenWriteTransaction())
                                {
                                    _database.Alerts.AddAlert(new Alert
                                    {
                                        Key = FromToString,
                                        Type = AlertType.Replication,
                                        Message = msg,
                                        CreatedAt = DateTime.UtcNow,
                                        Severity = AlertSeverity.Warning
                                    }, _configurationContext, txw);
                                    txw.Commit();
                                }

                                throw;
                            }
                            catch (Exception e)
                            {
                                var msg =
                                    $"Failed to parse initial server response. This is definitely not supposed to happen. Exception thrown: {e}";
                                if (_log.IsInfoEnabled)
                                    _log.Info(msg,e);

                                using (var txw = _configurationContext.OpenWriteTransaction())
                                {
                                    _database.Alerts.AddAlert(new Alert
                                    {
                                        Key = FromToString,
                                        Type = AlertType.Replication,
                                        Message = msg,
                                        CreatedAt = DateTime.UtcNow,
                                        Severity = AlertSeverity.Error
                                    }, _configurationContext, txw);
                                    txw.Commit();
                                }

                                throw;
                            }

                            while (_cts.IsCancellationRequested == false)
                            {
                                _parser.Reset();
                                _documentsContext.ResetAndRenew();                                
                                _parser.Renew();
                                long currentEtag;

                                Debug.Assert(_database.IndexMetadataPersistence.IsInitialized);

                                using (_configurationContext.OpenReadTransaction())
                                    currentEtag = _database.IndexMetadataPersistence.ReadLastEtag(_configurationContext.Transaction.InnerTransaction);

                               
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
                                while (WaitForChanges(_minimalHeartbeatInterval, _cts.Token) == false)
                                {
                                    _parser.Reset();
                                    _configurationContext.ResetAndRenew();
                                    _documentsContext.ResetAndRenew();
                                    _parser.Renew();
                                    using (_documentsContext.OpenReadTransaction())
                                    using (_configurationContext.OpenReadTransaction())
                                    {
                                        SendHeartbeat();
                                    }
                                }
                                _waitForChanges.Reset();
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Operation canceled on replication thread ({FromToString}). Stopped the thread.");
                Shutdown?.Invoke(this);
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
                Shutdown?.Invoke(this);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Unexpected exception occured on replication thread ({FromToString}). Replication stopped (will be retried later).", e);
                Failed?.Invoke(this, e);
            }
        }

        private bool WaitForChanges(int timeout, CancellationToken token)
        {
            while (true)
            {
                int state;
                using (var interruptibleParseToMemory = _parser.InterruptibleParseToMemory("replication notify message", _waitForChanges, timeout, token, out state))
                {
                    if (interruptibleParseToMemory != null)
                    {
                        using (_documentsContext.OpenReadTransaction())
                        using (_configurationContext.OpenReadTransaction())
                        {
                            HandleServerResponse(interruptibleParseToMemory, allowNotify: true);
                        }
                    }
                    else
                    {
                        return state != -1; // -1 is timeout
                    }
                }
            }
        }

        internal void WriteToServerAndFlush(DynamicJsonValue val)
        {
            _documentsContext.Write(_writer, val);
            _writer.Flush();
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            if (replicationBatchReply.MessageType == null)
                throw new InvalidOperationException("MessageType on replication response is null. This is likely is a symptom of an issue, and should be investigated.");

            _destinationLastKnownDocumentChangeVector.Clear();

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        private void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = Math.Max(_lastSentDocumentEtag, replicationBatchReply.LastEtagAccepted);
            _lastSentIndexOrTransformerEtag = Math.Max(_lastSentIndexOrTransformerEtag, replicationBatchReply.LastIndexTransformerEtagAccepted);
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

            //using (_documentsContext.OpenReadTransaction())
            {
                if (DocumentsStorage.ReadLastEtag(_documentsContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastEtagAccepted)
                {
                    // We have changes that the other side doesn't have, this can be because we have writes
                    // or because we have documents that were replicated to us. Either way, we need to sync
                    // those up with the remove side, so we'll start the replication loop again.
                    // We don't care if they are locally modified or not, because we filter documents that
                    // the other side already have (based on the change vector).
                    if ((DateTime.UtcNow - _lastDocumentSentTime).TotalMilliseconds > _minimalHeartbeatInterval)
                        _waitForChanges.SetByAsyncCompletion();
                }
            }

            if (
                _database.IndexMetadataPersistence.ReadLastEtag(_configurationContext.Transaction.InnerTransaction) !=
                replicationBatchReply.LastIndexTransformerEtagAccepted)
            {
                if ((DateTime.UtcNow - _lastIndexOrTransformerSentTime).TotalMilliseconds > _minimalHeartbeatInterval)
                    _waitForChanges.SetByAsyncCompletion();
            }
        }

        private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

        public ReplicationDestination Destination => _destination;

        internal void SendHeartbeat()
        {
            try
            {
                _documentsContext.Write(_writer, new DynamicJsonValue
                {
                    [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Heartbeat,
                    [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastSentDocumentEtag,
                    [nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag)] = _lastSentIndexOrTransformerEtag,
                    [nameof(ReplicationMessageHeader.ItemCount)] = 0                    
                });
                _writer.Flush();
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

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        internal Tuple<ReplicationMessageReply.ReplyType, ReplicationMessageReply> HandleServerResponse()
        {
            while (true)
            {
                using (var replicationBatchReplyMessage = _parser.InterruptibleParseToMemory("replication acknowledge message", _connectionDisposed))
                {
                    if (replicationBatchReplyMessage == null)
                        ThrowConnectionClosed();

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage, allowNotify: false);
                    if(replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

                    return Tuple.Create(replicationBatchReply.Type,
                        replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error
                            ? replicationBatchReply
                            : null);
                }
            }
        }

        private static void ThrowConnectionClosed()
        {
            throw new OperationCanceledException("The connection has been closed by the Dispose method");
        }

        internal ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage, bool allowNotify)
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
                    _log.Info($"Failed to connect to remote replication destination {connection.Url}. Socket Error Code = {e.SocketErrorCode}", e);
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
            if (IncomingReplicationHandler.IsIncomingReplicationThread
                && notification.Type != DocumentChangeTypes.DeleteOnTombstoneReplication)
                return;
            _waitForChanges.Set();
        }

        private void OnIndexChange(IndexChangeNotification notification)
        {
            if (notification.Type != IndexChangeTypes.IndexAdded &&
                notification.Type != IndexChangeTypes.IndexRemoved)
                return;            

            if(_log.IsInfoEnabled)
                _log.Info($"Received index {notification.Type} event, index name = {notification.Name}, etag = {notification.Etag}");

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
                _log.Info($"Received transformer {notification.Type} event, transformer name = {notification.Name}, etag = {notification.Etag}");

            if (IncomingReplicationHandler.IsIncomingReplicationThread)
                return;
            _waitForChanges.Set();
        }

        public void Dispose()
        {
            if(_log.IsInfoEnabled)
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