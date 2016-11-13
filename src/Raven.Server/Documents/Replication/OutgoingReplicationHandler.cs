using System;
using System.Collections.Generic;
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
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        internal readonly DocumentDatabase _database;
        internal readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim(false);
        private readonly CancellationTokenSource _cts;
        private readonly TimeSpan _minimalHeartbeatInterval = TimeSpan.FromSeconds(15);
        private Thread _sendingThread;

        internal long _lastSentDocumentEtag;
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
        internal DocumentsOperationContext _context;

        internal CancellationToken CancellationToken => _cts.Token;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

        public OutgoingReplicationHandler(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _database = database;
            _destination = destination;
            _log = LoggingSource.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _database.Notifications.OnDocumentChange += OnDocumentChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _sendingThread = new Thread(ReplicateDocuments)
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
                    _log.Info($"Will replicate to {_destination.Database} @ {_destination.Url} via tcp://{tcpConnectionInfo.Url}:{tcpConnectionInfo.Port}");
                }
                return tcpConnectionInfo;
            }
        }

        private void ReplicateDocuments()
        {
            try
            {
                var connectionInfo = GetTcpInfo();	            
                using (_tcpClient = new TcpClient())
                {
                    ConnectSocket(connectionInfo, _tcpClient);

                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context))
                    using (var stream = _tcpClient.GetStream())
                    {
                        var sender = new ReplicationDocumentSender(stream, this, _log);
                        using (_writer = new BlittableJsonTextWriter(_context, stream))
                        using (_parser = _context.ParseMultiFrom(stream))
                        {
                            //send initial connection information
                            _context.Write(_writer, new DynamicJsonValue
                            {
                                ["DatabaseName"] = _destination.Database,
                                ["Operation"] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                            });

                            //start request/response for fetching last etag
                            _context.Write(_writer, new DynamicJsonValue
                            {
                                ["Type"] = "GetLastEtag",
                                ["SourceDatabaseId"] = _database.DbId.ToString(),
                                ["SourceDatabaseName"] = _database.Name,
                                ["SourceUrl"] = _database.Configuration.Core.ServerUrl,
                                ["MachineName"] = Environment.MachineName,
                            });
                            _writer.Flush();
                            using (_context.OpenReadTransaction())
                            {
                                HandleServerResponse();
                            }

                            while (_cts.IsCancellationRequested == false)
                            {
                                _context.ResetAndRenew();
                                if (sender.ExecuteReplicationOnce() == false)
                                {
                                    using (_context.OpenReadTransaction())
                                    {
                                        var currentEtag =
                                            DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction);
                                        if (currentEtag < _lastSentDocumentEtag)
                                            continue;
                                    }
                                }

                                //if this returns false, this means either timeout or canceled token is activated                    
                                while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
                                {
                                    _context.ResetAndRenew();
                                    using (_context.OpenReadTransaction())
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
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Unexpected exception occured on replication thread ({FromToString}). Replication stopped (will be retried later).", e);
                Failed?.Invoke(this, e);
            }
        }

        internal void WriteToServerAndFlush(DynamicJsonValue val)
        {
            _context.Write(_writer,val);
            _writer.Flush();
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            _destinationLastKnownDocumentChangeVector.Clear();

            _lastSentDocumentEtag = replicationBatchReply.LastEtagAccepted;

            _destinationLastKnownDocumentChangeVectorAsString = replicationBatchReply.CurrentChangeVector.Format();

            foreach (var changeVectorEntry in replicationBatchReply.CurrentChangeVector)
            {
                _destinationLastKnownDocumentChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }
            if (DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction) >
                replicationBatchReply.LastEtagAccepted)
            {
                // We have changes that the other side doesn't have, this can be because we have writes
                // or because we have documents that were replicated to us. Either way, we need to sync
                // those up with the remove side, so we'll start the replication loop again.
                // We don't care if they are locally modified or not, because we filter documents that
                // the other side already have (based on the change vector).
                if (DateTime.UtcNow - _lastDocumentSentTime > _minimalHeartbeatInterval)
                    _waitForChanges.Set();
            }
        }

        private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

        public ReplicationDestination Destination => _destination;

        internal void SendHeartbeat()
        {
            try
            {
                _context.Write(_writer, new DynamicJsonValue
                {
                    ["Type"] = "ReplicationBatch",
                    ["LastDocumentEtag"] = _lastSentDocumentEtag,
                    ["LastIndexOrTransformerEtag"] = _lastSentIndexOrTransformerEtag,
                    ["Documents"] = 0,
                    ["IndexesAndTransformers"] = 0
                });
                _writer.Flush();
                HandleServerResponse();
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                throw;
            }
        }

        internal void HandleServerResponse()
        {
            using (var replicationBatchReplyMessage = _parser.ParseToMemory("replication acknowledge message"))
            {
                var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);

                if (replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Ok)
                {
                    UpdateDestinationChangeVector(replicationBatchReply);
                    OnSuccessfulTwoWaysCommunication();
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
                                $"Received reply for replication batch from {_destination.Database} at {_destination.Url}. There has been a failure, error string received : {replicationBatchReply.Error}");
                            throw new InvalidOperationException(
                                $"Received failure reply for replication batch. Error string received = {replicationBatchReply.Error}");
                        default:
                            throw new ArgumentOutOfRangeException(nameof(replicationBatchReply),
                                "Received reply for replication batch with unrecognized type... got " +
                                replicationBatchReply.Type);
                    }
                }
            }
        }

        private void ConnectSocket(TcpConnectionInfo connection, TcpClient tcpClient)
        {
            var host = new Uri(connection.Url).Host;
            try
            {
                tcpClient.ConnectAsync(host, connection.Port).Wait(CancellationToken);
            }
            catch (SocketException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {host}:{connection.Port}. Socket Error Code = {e.SocketErrorCode}", e);
                throw;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {host}:{connection.Port}", e);
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

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= OnDocumentChange;

            _cts.Cancel();
            try
            {
                _tcpClient?.Dispose();
            }
            catch (Exception) { }

            if (_sendingThread != Thread.CurrentThread)
            {
                _sendingThread?.Join();
            }
        }

        private void OnSuccessfulTwoWaysCommunication() => SuccessfulTwoWaysCommunication?.Invoke(this);

    }
}