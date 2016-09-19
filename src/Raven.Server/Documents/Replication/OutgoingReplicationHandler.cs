using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        private DateTime _lastSentTime;
        private readonly Dictionary<Guid, long> _destinationLastKnownChangeVector = new Dictionary<Guid, long>();
        private string _destinationLastKnownChangeVectorString;
        private TcpClient _tcpClient;
        private BlittableJsonTextWriter _writer;
        private JsonOperationContext.MultiDocumentParser _parser;
        private DocumentsOperationContext _context;

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
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context))
                    using (var sender = new ReplicationSender(this, _tcpClient, _log))
                    using (_writer = new BlittableJsonTextWriter(_context, sender.Stream))
                    using (_parser = _context.ParseMultiFrom(sender.Stream))
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
                            _context.Reset();							
                            if (sender.ExecuteReplicationOnce() == false)
                            {
                                using (_context.OpenReadTransaction())
                                {
                                    var currentEtag = DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction);
                                    if (currentEtag < _lastSentEtag)
                                        continue;
                                }
                            }

                            //if this returns false, this means either timeout or canceled token is activated                    
                            while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
                            {
                                _context.Reset();
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

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            _destinationLastKnownChangeVector.Clear();

            _lastSentEtag = replicationBatchReply.LastEtagAccepted;

            _destinationLastKnownChangeVectorString = replicationBatchReply.CurrentChangeVector.Format();

            foreach (var changeVectorEntry in replicationBatchReply.CurrentChangeVector)
            {
                _destinationLastKnownChangeVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }
            if (DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction) >
                replicationBatchReply.LastEtagAccepted)
            {
                // We have changes that the other side doesn't have, this can be because we have writes
                // or because we have documents that were replicated to us. Either way, we need to sync
                // those up with the remove side, so we'll start the replication loop again.
                // We don't care if they are locally modified or not, because we filter documents that
                // the other side already have (based on the change vector).
                if (DateTime.UtcNow - _lastSentTime > _minimalHeartbeatInterval)
                    _waitForChanges.Set();
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
                    ["Type"] = "ReplicationBatch",
                    ["LastEtag"] = _lastSentEtag,
                    ["Documents"] = 0
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

        private class ReplicationSender : IDisposable
        {
            private readonly OutgoingReplicationHandler _parent;
            private readonly Logger _log;
            private readonly DocumentsOperationContext _context;
            private long _lastEtag;
            private readonly SortedList<long, ReplicationBatchItem> _orderedReplicaItems;
            private readonly byte[] _tempBuffer = new byte[32 * 1024];
            private readonly NetworkStream _stream;

            public ReplicationSender(OutgoingReplicationHandler parent, TcpClient tcpClient, Logger log)
            {
                _parent = parent;
                _log = log;
                _context = _parent._context;
                _orderedReplicaItems = new SortedList<long, ReplicationBatchItem>();
                _stream = tcpClient.GetStream();
            }

            public NetworkStream Stream => _stream;

            public bool ExecuteReplicationOnce()
            {
                _orderedReplicaItems.Clear();
                var readTx = _context.OpenReadTransaction();
                try
                {

                    // we scan through the documents to send to the other side, we need to be careful about
                    // filtering a lot of documents, because we need to let the other side know about this, and 
                    // at the same time, we need to send a heartbeat to keep the tcp connection alive
                    var sp = Stopwatch.StartNew();
                    var timeout = Debugger.IsAttached ? 60 * 1000 : 1000;
                    while (sp.ElapsedMilliseconds < timeout)
                    {
                        _lastEtag = _parent._lastSentEtag;

                        _parent._cts.Token.ThrowIfCancellationRequested();

                        var docs = _parent._database.DocumentsStorage.GetDocumentsAfter(_context, _lastEtag + 1, 0, 1024)
                            .ToList();
                        var tombstones = _parent._database.DocumentsStorage.GetTombstonesAfter(_context, _lastEtag + 1, 0, 1024)
                            .ToList();
                        
                        long maxEtag;
                        maxEtag = _lastEtag;
                        if (docs.Count > 0)
                        {
                            maxEtag = docs[docs.Count - 1].Etag;
                        }

                        if (tombstones.Count > 0)
                        {
                            maxEtag = Math.Max(maxEtag, tombstones[tombstones.Count - 1].Etag);
                        }

                        foreach (var doc in docs)
                        {
                            if (doc.Etag > maxEtag)
                                break;
                            AddReplicationItemToBatch(new ReplicationBatchItem
                            {
                                Etag = doc.Etag,
                                ChangeVector = doc.ChangeVector,
                                Data = doc.Data,
                                Key = doc.Key
                            });
                        }

                        foreach (var tombstone in tombstones)
                        {
                            if (tombstone.Etag > maxEtag)
                                break;
                            AddReplicationItemToBatch(new ReplicationBatchItem
                            {
                                Etag = tombstone.Etag,
                                ChangeVector = tombstone.ChangeVector,
                                Collection = tombstone.Collection,
                                Key = tombstone.Key
                            });
                        }

                        // if we are at the end, we are done
                        if (_lastEtag <= DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction))
                        {
                            break;
                        }
                    }

                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Found {_orderedReplicaItems.Count:#,#;;0} documents to replicate to {_parent._destination.Database} @ {_parent._destination.Url} in {sp.ElapsedMilliseconds:#,#;;0} ms.");
                    }

                    if (_orderedReplicaItems.Count == 0)
                    {
                        var hasModification = _lastEtag != _parent._lastSentEtag;
                        _parent._lastSentEtag = _lastEtag;
                        // ensure that the other server is aware that we skipped 
                        // on (potentially a lot of) documents to send, and we update
                        // the last etag they have from us on the other side
                        _parent.SendHeartbeat();
                        return hasModification;
                    }

                    _parent._cts.Token.ThrowIfCancellationRequested();

                    SendDocuments();
                    return true;
                }
                finally
                {
                    if (readTx.Disposed == false)
                        readTx.Dispose();
                }
            }


            private void AddReplicationItemToBatch(ReplicationBatchItem item)
            {
                if (ShouldSkipReplication(item.Key))
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Skipping replication of {item.Key} because it is a system document");
                    }
                    return;
                }
                // destination already has it
                if (item.ChangeVector.GreaterThan(_parent._destinationLastKnownChangeVector) == false)
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Skipping replication of {item.Key} because destination has a higher change vector. Doc: {item.ChangeVector.Format()} < Dest: {_parent._destinationLastKnownChangeVectorString} ");
                    }
                    return;
                }
                _lastEtag = Math.Max(_lastEtag, item.Etag);
                _orderedReplicaItems.Add(item.Etag, item);
            }


            private void SendDocuments()
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

                var sw = Stopwatch.StartNew();
                var headerJson = new DynamicJsonValue
                {
                    ["Type"] = "ReplicationBatch",
                    ["LastEtag"] = _lastEtag,
                    ["Documents"] = _orderedReplicaItems.Count
                };
                _context.Write(_parent._writer, headerJson);
                _parent._writer.Flush();

                foreach (var item in _orderedReplicaItems)
                {
                    WriteDocumentToServer(item.Value.Key, item.Value.ChangeVector, item.Value.Data, item.Value.Collection);
                }

                // we can release the read transaction while we are waiting for 
                // reply from the server and not hold it for a long time
                _context.Transaction.Dispose();

                _stream.Flush();
                sw.Stop();

                _parent._lastSentEtag = _lastEtag;

                if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                    _log.Info(
                        $"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. First sent etag = {_orderedReplicaItems[0].Etag}, last sent etag = {_lastEtag}");
                _parent._lastSentTime = DateTime.UtcNow;
                using (_context.OpenReadTransaction())
                {
                    _parent.HandleServerResponse();
                }
            }

            private unsafe void WriteDocumentToServer(
                LazyStringValue key,
                ChangeVectorEntry[] changeVector,
                BlittableJsonReaderObject data,
                LazyStringValue collection)
            {
                var changeVectorSize = changeVector.Length * sizeof(ChangeVectorEntry);
                var requiredSize = changeVectorSize +
                                   sizeof(int) + // # of change vectors
                                   sizeof(int) + // size of document key
                                   key.Size +
                                   sizeof(int) // size of document
                    ;
                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(key, changeVector);

                fixed (byte* pTemp = _tempBuffer)
                {
                    int tempBufferPos = 0;
                    fixed (ChangeVectorEntry* pChangeVectorEntries = changeVector)
                    {
                        *(int*) pTemp = changeVector.Length;
                        tempBufferPos += sizeof(int);
                        Memory.Copy(pTemp + tempBufferPos, (byte*) pChangeVectorEntries, changeVectorSize);
                        tempBufferPos += changeVectorSize;
                    }
                    *(int*) (pTemp + tempBufferPos) = key.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, key.Buffer, key.Size);
                    tempBufferPos += key.Size;

                    //if data == null --> this is a tombstone, and a document otherwise
                    if (data != null)
                    {
                        *(int*) (pTemp + tempBufferPos) = data.Size;
                        tempBufferPos += sizeof(int);

                        var docReadPos = 0;
                        while (docReadPos < data?.Size)
                        {
                            var sizeToCopy = Math.Min(data.Size - docReadPos, _tempBuffer.Length - tempBufferPos);
                            if (sizeToCopy == 0) // buffer is full, need to flush it
                            {
                                _stream.Write(_tempBuffer, 0, tempBufferPos);
                                tempBufferPos = 0;
                                continue;
                            }
                            Memory.Copy(pTemp + tempBufferPos, data.BasePointer + docReadPos, sizeToCopy);
                            tempBufferPos += sizeToCopy;
                            docReadPos += sizeToCopy;
                        }
                    }
                    else
                    {
                        //tombstone have size == -1
                        *(int*) (pTemp + tempBufferPos) = -1;
                        tempBufferPos += sizeof(int);

                        if (collection == null) //precaution
                        {
                            throw new InvalidDataException("Cannot write tombstone with empty collection name...");
                        }

                        *(int*)(pTemp + tempBufferPos) = collection.Size;
                        tempBufferPos += sizeof(int);
                        Memory.Copy(pTemp + tempBufferPos, collection.Buffer, collection.Size);
                        tempBufferPos += collection.Size;
                    }
                    _stream.Write(_tempBuffer, 0, tempBufferPos);
                }
            }

            public void Dispose()
            {
                _stream.Dispose();
            }
        }

        private static void ThrowTooManyChangeVectorEntries(LazyStringValue key, ChangeVectorEntry[] changeVector)
        {
            throw new ArgumentOutOfRangeException("doc",
                "Document " + key + " has too many change vector entries to replicate: " +
                changeVector.Length);
        }


        private void HandleServerResponse()
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