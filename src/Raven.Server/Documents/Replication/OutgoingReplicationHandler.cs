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
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.Server.Extensions;
using Sparrow;

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
        private string _destinationLastKnownChangeVectorString;
        private TcpClient _tcpClient;
        private NetworkStream _stream;
        private BlittableJsonTextWriter _writer;
        private JsonOperationContext.MultiDocumentParser _parser;
        private DocumentsOperationContext _context;
        private readonly byte[] _tempBuffer = new byte[32 * 1024];

        public event Action<OutgoingReplicationHandler, Exception> Failed;

        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;

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
                    _stream = _tcpClient.GetStream();
                    using (_stream)
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context))
                    using (_writer = new BlittableJsonTextWriter(_context, _stream))
                    using (_parser = _context.ParseMultiFrom(_stream))
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

                        using (var lastEtagMessage = _parser.ParseToMemory($"Last etag from server {FromToString}"))
                        {
                            var replicationEtagReply = JsonDeserializationServer.ReplicationEtagReply(lastEtagMessage);
                            _lastSentEtag = replicationEtagReply.LastSentEtag;
                            UpdateDestinationChangeVector(replicationEtagReply.CurrentChangeVector);
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(
                                    $"Connected to {_destination.Database} @ {_destination.Url}, last sent etag {replicationEtagReply.LastSentEtag}. Change vector: [{replicationEtagReply.CurrentChangeVector.Format()}]");
                            }
                        }

                        while (_cts.IsCancellationRequested == false)
                        {
                            _context.Reset();

                            if (ExecuteReplicationOnce() == false)
                            {
                                using (_context.OpenReadTransaction())
                                {
                                    if (DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction) < _lastSentEtag)
                                        continue;
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
                            {
                                _context.Reset();
                                SendHeartbeat();
                            }
                            _waitForChanges.Reset();
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

        private void UpdateDestinationChangeVector(ChangeVectorEntry[] currentChangeVector)
        {
            _destinationLastKnownChangeVector.Clear();
            _destinationLastKnownChangeVectorString = currentChangeVector.Format();
            foreach (var changeVectorEntry in currentChangeVector)
            {
                _destinationLastKnownChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }
        }

        private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

        public ReplicationDestination Destination => _destination;

        private void SendHeartbeat()
        {
            try
            {
                _context.Write(_writer, new DynamicJsonValue
                {
                    ["Heartbeat"] = true,
                    ["LastEtag"] = _lastSentEtag
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

        private static unsafe bool ShouldSkipReplication(LazyStringValue str)
        {
            if (str.Length < 6)
                return false;

            // case insensitive 'Raven/' match without doing allocations

            if ((str.Buffer[0] != (byte)'R' && str.Buffer[0] != (byte)'r') ||
                (str.Buffer[1] != (byte)'A' && str.Buffer[1] != (byte)'a') ||
                (str.Buffer[2] != (byte)'V' && str.Buffer[2] != (byte)'v') ||
                (str.Buffer[3] != (byte)'E' && str.Buffer[3] != (byte)'e') ||
                (str.Buffer[4] != (byte)'N' && str.Buffer[4] != (byte)'n') ||
                 str.Buffer[5] != (byte)'/')
                return false;

            if (str.Length < 11)
                return true;

            // Now need to find if the next bits are 'hilo/'
            if ((str.Buffer[6] == (byte)'H' || str.Buffer[0] == (byte)'h') &&
                (str.Buffer[7] == (byte)'I' || str.Buffer[1] == (byte)'i') &&
                (str.Buffer[8] == (byte)'L' || str.Buffer[2] == (byte)'l') &&
                (str.Buffer[9] == (byte)'O' || str.Buffer[3] == (byte)'o') &&
                str.Buffer[10] == (byte)'/')
                return false;

            return true;
        }

        private bool ExecuteReplicationOnce()
        {
            using (_context.OpenReadTransaction())
            {
                var replicationBatch = new List<Document>();
                var lastEtag = _lastSentEtag;

                // we scan through the documents to send to the other side, we need to be careful about
                // filtering a lot of documents, because we need to let the other side know about this, and 
                // at the same time, we need to send a heartbeat to keep the tcp connection alive
                var sp = Stopwatch.StartNew();
                var timeout = Debugger.IsAttached ? 60 * 1000 : 1000;
                while (sp.ElapsedMilliseconds < timeout)
                {
                    _cts.Token.ThrowIfCancellationRequested();
                    foreach (var document in _database.DocumentsStorage.GetDocumentsAfter(_context, lastEtag, 0, 1024))
                    {
                        if (sp.ElapsedMilliseconds > timeout)
                            break;

                        lastEtag = document.Etag;

                        if (ShouldSkipReplication(document.Key))
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info($"Skipping replication of {document.Key} because it is a system document");
                            }
                            continue;
                        }

                        // destination already has it
                        if (document.ChangeVector.GreaterThen(_destinationLastKnownChangeVector) == false)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info($"Skipping replication of {document.Key} because destination has a higher change vector. Doc: {document.ChangeVector.Format()} < Dest: {_destinationLastKnownChangeVectorString} ");
                            }
                            continue;
                        }

                        replicationBatch.Add(document);
                    }

                    if (replicationBatch.Count != 0)
                        break;

                    // if we are at the end, we are done
                    if (lastEtag == DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction))
                        break;
                }
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Found {replicationBatch.Count:#,#;;0} documents to replicate to {_destination.Database} @ {_destination.Url} in {sp.ElapsedMilliseconds:#,#;;0} ms.");
                }
                if (replicationBatch.Count == 0)
                {
                    var hasModification = lastEtag != _lastSentEtag;
                    _lastSentEtag = lastEtag;
                    // ensure that the other server is aware that we skipped 
                    // on (potentially a lot of) documents to send, and we update
                    // the last etag they have from us on the other side
                    SendHeartbeat();
                    return hasModification;
                }

                _cts.Token.ThrowIfCancellationRequested();

                SendDocuments(replicationBatch, lastEtag);
                return true;
            }
        }

        private void SendDocuments(
            List<Document> docs,
            long lastEtag)
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_database.Name}) with {docs.Count:#,#;;0} docs and last etag {lastEtag}");

            var sw = Stopwatch.StartNew();
            _context.Write(_writer, new DynamicJsonValue
            {
                ["Type"] = "ReplicationBatch",
                ["LastEtag"] = lastEtag,
                ["Documents"] = docs.Count
            });
            _writer.Flush();
            foreach (var document in docs)
            {
                WriteDocumentToServer(document);
            }
            _stream.Flush();
            sw.Stop();

            _lastSentEtag = lastEtag;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Finished sending replication batch. Sent {docs.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. First sent etag = {docs[0].Etag}, last sent etag = {lastEtag}");

            HandleServerResponse();
        }

        private unsafe void WriteDocumentToServer(Document doc)
        {
            var changeVectorSize = doc.ChangeVector.Length * sizeof(ChangeVectorEntry);
            var requiredSize = changeVectorSize +
                               sizeof(int) + // # of change vectors
                               sizeof(int) + // size of document key
                               doc.Key.Size +
                               sizeof(int) // size of document
                ;
            if (requiredSize > _tempBuffer.Length)
                ThrowTooManyChangeVectorEntries(doc);

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                fixed (ChangeVectorEntry* pChangeVectorEntries = doc.ChangeVector)
                {
                    *(int*)pTemp = doc.ChangeVector.Length;
                    tempBufferPos += sizeof (int);
                    Memory.Copy(pTemp + tempBufferPos, (byte*)pChangeVectorEntries, changeVectorSize);
                    tempBufferPos += changeVectorSize;
                }
                *(int*)(pTemp + tempBufferPos) = doc.Key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, doc.Key.Buffer, doc.Key.Size);
                tempBufferPos += doc.Key.Size;
                *(int*)(pTemp + tempBufferPos) = doc.Data.Size;
                tempBufferPos += sizeof(int);
                var docReadPos = 0;
                while (docReadPos < doc.Data.Size)
                {
                    var sizeToCopy = Math.Min(doc.Data.Size - docReadPos, _tempBuffer.Length - tempBufferPos);
                    if (sizeToCopy == 0) // buffer is full, need to flush it
                    {
                        _stream.Write(_tempBuffer, 0, tempBufferPos);
                        tempBufferPos = 0;
                        continue;
                    }
                    Memory.Copy(pTemp + tempBufferPos, doc.Data.BasePointer + docReadPos, sizeToCopy);
                    tempBufferPos += sizeToCopy;
                    docReadPos += sizeToCopy;
                }
                if (tempBufferPos != 0)
                {
                    _stream.Write(_tempBuffer, 0, tempBufferPos);
                }
            }
        }

        private static void ThrowTooManyChangeVectorEntries(Document doc)
        {
            throw new ArgumentOutOfRangeException(nameof(doc),
                "Document " + doc.Key + " has too many change vector entries to replicate: " +
                doc.ChangeVector.Length);
        }


        private void HandleServerResponse()
        {
            using (var replicationBatchReplyMessage = _parser.ParseToMemory("replication acknowledge message"))
            {
                var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);

                if (replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Ok)
                {
                    UpdateDestinationChangeVector(replicationBatchReply.CurrentChangeVector);
                    OnSuccessfulTwoWaysCommunication();
                }

                if (_log.IsInfoEnabled)
                {
                    switch (replicationBatchReply.Type)
                    {
                        case ReplicationMessageReply.ReplyType.Ok:
                            _log.Info(
                                $"Received reply for replication batch from {_destination.Database} @ {_destination.Url}. New destination change vector is {_destinationLastKnownChangeVectorString}");
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

        private void OnSuccessfulTwoWaysCommunication() => SuccessfulTwoWaysCommunication?.Invoke(this);
    }
}