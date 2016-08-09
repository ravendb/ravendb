using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using System.Runtime.CompilerServices;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim(false);
        private readonly CancellationTokenSource _cts;
        private readonly TimeSpan _minimalHeartbeatInterval = TimeSpan.FromSeconds(15);
        private Thread _sendingThread;

        private long _lastSentEtag;
        private readonly Dictionary<Guid, long> _destinationLastKnownChangeVector = new Dictionary<Guid, long>();
        private TcpClient _tcpClient;

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> DocumentsSent;

        public OutgoingReplicationHandler(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _database = database;
            _destination = destination;
            _log = LoggerSetup.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);
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
                new OperationCredentials(_destination.ApiKey, CredentialCache.DefaultCredentials), convention)))
            {
                var result = request.ReadResponseJson();
                return convention.CreateSerializer().Deserialize<TcpConnectionInfo>(new RavenJTokenReader(result));
            }
        }

        //TODO : add code to record stats and maybe additional logging
        private void ReplicateDocuments()
        {
            try
            {
                var connectionInfo = GetTcpInfo();
                using (_tcpClient = new TcpClient())
                {
                    DocumentsOperationContext context;
                    ConnectSocket(connectionInfo, _tcpClient);
                    using (var stream = _tcpClient.GetStream())
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    using (var parser = context.ParseMultiFrom(stream))
                    {
                        //send initial connection information
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["DatabaseName"] = _destination.Database,
                            ["Operation"] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
                        });

                        //start request/response for fetching last etag
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Type"] = "GetLastEtag",
                            ["SourceDatabaseId"] = _database.DbId.ToString(),
                            ["SourceDatabaseName"] = _database.Name,
                            ["SourceUrl"] = _database.Configuration.Core.ServerUrl,
                            ["MachineName"] = Environment.MachineName,
                        });
                        writer.Flush();

                        using (var lastEtagMessage = parser.ParseToMemory($"Last etag from server {FromToString}"))
                        {
                            var replicationEtagReply = JsonDeserializationServer.ReplicationEtagReply(lastEtagMessage);
                            _lastSentEtag = replicationEtagReply.LastSentEtag;
                            _destinationLastKnownChangeVector.Clear();
                            foreach (var changeVectorEntry in replicationEtagReply.CurrentChangeVector)
                            {
                                _destinationLastKnownChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                            }
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            if (ExecuteReplicationOnce(context, writer, parser) == false)
                            {
                                using (context.OpenReadTransaction())
                                {
                                    if (DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction) < _lastSentEtag)
                                        continue;
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
                            {
                                SendHeartbeat(context, writer);
                            }
                            _waitForChanges.Reset();
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Operation canceled on replication thread ({FromToString}). Stopped the thread.", null);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Unexpected exception occured on replication thread ({FromToString}). Stopped the thread.", e);
                Failed?.Invoke(this, e);
            }
        }

        private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

        public ReplicationDestination Destination => _destination;

        private void SendHeartbeat(DocumentsOperationContext context, BlittableJsonTextWriter writer)
        {
            try
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Heartbeat"] = true,
                    ["LastEtag"] = _lastSentEtag
                });
                writer.Flush();

                //TODO: Get the reply from the destination and set _destinationLastKnownChangeVector
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                throw;
            }
        }


        private bool ExecuteReplicationOnce(DocumentsOperationContext context,
            BlittableJsonTextWriter writer,
            JsonOperationContext.MultiDocumentParser parser)
        {
            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
            using (context.OpenReadTransaction())
            {
                //TODO: make replication batch size configurable
                //also, perhaps there should be timers/heuristics
                //that would dynamically resize batch size

                var replicationBatch = new List<Document>();
                var lastEtag = _lastSentEtag;

                // we scan through the documents to send to the other side, we need to be careful about
                // filtering a lot of documents, because we need to let the other side know about this, and 
                // at the same time, we need to send a heartbeat to keep the tcp connection alive
                var sp = Stopwatch.StartNew();
                while(sp.ElapsedMilliseconds < 500)
                {
                    _cts.Token.ThrowIfCancellationRequested();
                    foreach (var document in documentStorage.GetDocumentsAfter(context, lastEtag, 0, 1024))
                    {
                        lastEtag = document.Etag;

                        //TODO: check the lazy string directly
                        var key = document.Key.ToString();
                        if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) &&
                            key.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false)
                        {
                            continue;
                        }

                        // destination already has it
                        if (document.ChangeVector.GreaterThen(_destinationLastKnownChangeVector) == false)
                            continue;

                        replicationBatch.Add(document);
                    }

                    if (replicationBatch.Count != 0)
                        break;

                    // if we are at the end, we are done
                    if (lastEtag == DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction))
                        break;
                }

                if (replicationBatch.Count == 0)
                {
                    var hasModification = lastEtag != _lastSentEtag;
                    _lastSentEtag = lastEtag;
                    // ensure that the other server is aware that we skipped 
                    // on (potentially a lot of) documents to send, and we update
                    // the last etag they have from us on the other side
                    SendHeartbeat(context, writer);
                    return hasModification;
                }

                _cts.Token.ThrowIfCancellationRequested();

                SendDocuments(context, writer, parser, replicationBatch, lastEtag);
                return true;
            }
        }

        //TODO: add replication batch format in comments here		
        private void SendDocuments(
            DocumentsOperationContext context,
            BlittableJsonTextWriter writer,
            JsonOperationContext.MultiDocumentParser parser,
            List<Document> docs,
            long lastEtag)
        {
            if (docs == null) //precaution, should never happen
                throw new ArgumentNullException(nameof(docs));

            if (_log.IsInfoEnabled)
                _log.Info($"Starting sending replication batch ({_database.Name})");

            var sw = Stopwatch.StartNew();
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Type"));
            writer.WriteString(context.GetLazyStringForFieldWithCaching("ReplicationBatch"));
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("LastEtag"));
            writer.WriteInteger(lastEtag);
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("ReplicationBatch"));
            writer.WriteDocuments(context, docs, false);
            writer.WriteEndObject();
            writer.Flush();
            sw.Stop();

            _lastSentEtag = lastEtag;

            // number of docs, first / last etag, size, time
            if (_log.IsInfoEnabled)
                _log.Info($"Finished sending replication batch. Sent {docs.Count} documents in {sw.ElapsedMilliseconds} ms. First sent etag = {docs[0].Etag}, last sent etag = {lastEtag}");

            using (var replicationBatchReplyMessage = parser.ParseToMemory("replication acknowledge message"))
            {
                var replicationBatchReply = JsonDeserializationServer.ReplicationBatchReply(replicationBatchReplyMessage);


                if (replicationBatchReply.Type == ReplicationBatchReply.ReplyType.Ok)
                {
                    _destinationLastKnownChangeVector.Clear();
                    foreach (var changeVectorEntry in replicationBatchReply.CurrentChangeVector)
                    {
                        _destinationLastKnownChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }
                    OnDocumentsSent();
                }

                if (_log.IsInfoEnabled)
                {
                    switch (replicationBatchReply.Type)
                    {
                        case ReplicationBatchReply.ReplyType.Ok:
                            _log.Info($"Received reply for replication batch from {_destination.Database} at {_destination.Url}. Everything is ok.");

                            break;
                        case ReplicationBatchReply.ReplyType.Error:
                            _log.Info(
                                $"Received reply for replication batch from {_destination.Database} at {_destination.Url}. There has been a failure, error string received : {replicationBatchReply.Error}");
                            throw new InvalidOperationException($"Received failure reply for replication batch. Error string received = {replicationBatchReply.Error}");
                        default:
                            throw new ArgumentOutOfRangeException(nameof(replicationBatchReply), "Received reply for replication batch with unrecognized type... got " + replicationBatchReply.Type);
                    }
                }
            }
        }

        private void ConnectSocket(TcpConnectionInfo connection, TcpClient tcpClient)
        {
            var host = new Uri(connection.Url).Host;
            try
            {
                tcpClient.ConnectAsync(host, connection.Port).Wait();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnDocumentChange(DocumentChangeNotification notification) => _waitForChanges.Set();

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnDocumentsSent() => DocumentsSent?.Invoke(this);
    }
}