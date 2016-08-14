using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Linq;
using System.Text;
using NLog.Targets.Wrappers;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private NetworkStream _stream;
        private DocumentsOperationContext _context;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        private readonly IDisposable _contextDisposable;

        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;

        public IncomingReplicationHandler(JsonOperationContext.MultiDocumentParser multiDocumentParser, DocumentDatabase database, TcpClient tcpClient, NetworkStream stream, ReplicationLatestEtagRequest replicatedLastEtag)
        {
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            _multiDocumentParser = multiDocumentParser;
            _database = database;
            _tcpClient = tcpClient;
            _stream = stream;
            _contextDisposable = _database.DocumentsStorage
                                          .ContextPool
                                          .AllocateOperationContext(out _context);

            _log = LoggerSetup.Instance.GetLogger<IncomingReplicationHandler>(_database.Name);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _incomingThread = new Thread(ReceiveReplicatedDocuments)
            {
                IsBackground = true,
                Name = $"Incoming replication {FromToString}"
            };
            _incomingThread.Start();
            if (_log.IsInfoEnabled)
                _log.Info($"Incoming replication thread started ({FromToString})");

        }

        private void ReceiveReplicatedDocuments()
        {
            bool exceptionLogged = false;
            try
            {
                using (_contextDisposable)
                using (_stream)
                using (var writer = new BlittableJsonTextWriter(_context, _stream))
                using (_multiDocumentParser)
                {
                    long prevRecievedEtag = -1;
                    while (!_cts.IsCancellationRequested)
                    {
                        _context.Reset();

                        using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
                        {
                            //note: at this point, the valid messages are heartbeat and replication batch.
                            _cts.Token.ThrowIfCancellationRequested();

                            long lastReceivedEtag;
                            if (message.TryGet("LastEtag", out lastReceivedEtag) == false)
                                throw new InvalidDataException("The property 'LastEtag' wasn't found in replication batch, invalid data");

                            bool _;
                            if (message.TryGet("Heartbeat", out _))
                            {
                                prevRecievedEtag = HandleHeartbeat(lastReceivedEtag, prevRecievedEtag, writer);
                                continue;
                            }

                            int replicatedDocs = ValidateReplicationBatchAndGetDocsCount(message);

                            try
                            {
                                prevRecievedEtag = ReceiveSingleBatch(replicatedDocs, lastReceivedEtag, prevRecievedEtag);

                                OnDocumentsReceived(this);

                                //return positive ack
                                SendStatusToSource(writer, lastReceivedEtag);
                            }
                            catch (Exception e)
                            {
                                //if we are disposing, ignore errors
                                if (!_cts.IsCancellationRequested && !(e is ObjectDisposedException))
                                {
                                    //return negative ack
                                    _context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = ReplicationMessageReply.ReplyType.Error.ToString(),
                                        ["LastEtagAccepted"] = -1,
                                        ["Error"] = e.ToString()
                                    });

                                    exceptionLogged = true;

                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Failed replicating documents from {FromToString}.",e);
                                    throw;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (!_cts.IsCancellationRequested)
                {
                    if (_log.IsInfoEnabled && exceptionLogged == false)
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.",e );

                    OnFailed(e, this);
                }
            }
        }

        private long HandleHeartbeat(long lastReceivedEtag, long prevRecievedEtag, BlittableJsonTextWriter writer)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Got heartbeat from ({FromToString}) with etag {lastReceivedEtag}.");
            if (prevRecievedEtag != lastReceivedEtag)
            {
                using (_context.OpenWriteTransaction())
                {
                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context,
                        ConnectionInfo.SourceDatabaseId, lastReceivedEtag);
                    prevRecievedEtag = lastReceivedEtag;
                    _context.Transaction.Commit();
                }
            }
            SendStatusToSource(writer, -1);
            return prevRecievedEtag;
        }

        private unsafe long ReceiveSingleBatch(int replicatedDocs, long lastReceivedEtag, long prevRecievedEtag)
        {
            var sw = Stopwatch.StartNew();
            using (var writeBuffer = _context.GetStream())
            {

                // this will read the documents to memory from the network
                // without holding the write tx open
                ReadDocumentsFromSource(writeBuffer, replicatedDocs);
                byte* buffer;
                int _;
                writeBuffer.EnsureSingleChunk(out buffer, out _);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {replicatedDocs:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (_context.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(_context))
                    {
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }

                    foreach (var doc in _replicatedDocs)
                    {
                        if (_tempReplicatedChangeVector.Length != doc.ChangeVectorCount)
                        {
                            _tempReplicatedChangeVector = new ChangeVectorEntry[doc.ChangeVectorCount];
                        }
                        for (int i = 0; i < doc.ChangeVectorCount; i++)
                        {
                            _tempReplicatedChangeVector[i] = ((ChangeVectorEntry*) (buffer + doc.Position))[i];
                            long etag;
                            if (
                                maxReceivedChangeVectorByDatabase.TryGetValue(_tempReplicatedChangeVector[i].DbId,
                                    out etag) ==
                                false ||
                                etag > _tempReplicatedChangeVector[i].Etag)
                            {
                                maxReceivedChangeVectorByDatabase[_tempReplicatedChangeVector[i].DbId] = etag;
                            }
                        }

                        var json = new BlittableJsonReaderObject(
                            buffer + doc.Position + (doc.ChangeVectorCount*sizeof (ChangeVectorEntry))
                            , doc.DocumentSize, _context);

                        //TODO: conflict handling
                        _database.DocumentsStorage.Put(_context, doc.Id, null, json, _tempReplicatedChangeVector);
                    }
                    _database.DocumentsStorage.SetDatabaseChangeVector(_context,
                        _database.DocumentsStorage.GetDatabaseChangeVector(_context));

                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId,
                        lastReceivedEtag);
                    prevRecievedEtag = lastReceivedEtag;
                    _context.Transaction.Commit();
                }
                sw.Stop();

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received and written {replicatedDocs:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastReceivedEtag}.");
                return prevRecievedEtag;
            }
        }

        private void SendStatusToSource(BlittableJsonTextWriter writer, long lastReceivedEtag)
        {
            var changeVector = new DynamicJsonArray();
            ChangeVectorEntry[] databaseChangeVector;

            using (_context.OpenReadTransaction())
            {
                databaseChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(_context);
            }

            foreach (var changeVectorEntry in databaseChangeVector)
            {
                changeVector.Add(new DynamicJsonValue
                {
                    ["DbId"] = changeVectorEntry.DbId,
                    ["Etag"] = changeVectorEntry.Etag
                });
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"Sending ok to {FromToString} with last etag {lastReceivedEtag} and change vector: {databaseChangeVector.Format()}");
            }
            _context.Write(writer, new DynamicJsonValue
            {
                ["Type"] = "Ok",
                ["LastEtagAccepted"] = lastReceivedEtag,
                ["Error"] = null,
                ["CurrentChangeVector"] = changeVector
            });

            writer.Flush();
        }

        private static int ValidateReplicationBatchAndGetDocsCount(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

            if (!messageType.Equals("ReplicationBatch", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Expected the message 'Type = ReplicationBatch' field, but has 'Type={messageType}'. This is likely a bug.");

            int docs;
            if(!message.TryGet("Documents", out docs))
                throw new InvalidDataException($"Expected the 'Documents' field, but had no numeric field of this value, this is likely a bug");

            return docs;
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private readonly byte[] _tempBuffer = new byte[32*1024];
        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>(); 

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public int DocumentSize;
        }

        private unsafe void ReadDocumentsFromSource(UnmanagedWriteBuffer writeBuffer, int replicatedDocs)
        {
            _replicatedDocs.Clear();

            fixed (byte* pTemp = _tempBuffer)
            {
                for (int x = 0; x < replicatedDocs; x++)
                {
                    var curDoc = new ReplicationDocumentsPositions
                    {
                        Position = writeBuffer.SizeInBytes
                    };
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    curDoc.ChangeVectorCount = *(int*) pTemp;
                   
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(ChangeVectorEntry) * curDoc.ChangeVectorCount);
                    writeBuffer.Write(_tempBuffer, 0, sizeof (ChangeVectorEntry)*curDoc.ChangeVectorCount);
                    
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var keySize = *(int*)pTemp;
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, keySize);
                    curDoc.Id = Encoding.UTF8.GetString(_tempBuffer, 0, keySize);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var documentSize = curDoc.DocumentSize = *(int*)pTemp;
                    while (documentSize>0)
                    {
                        var read = _multiDocumentParser.Read(_tempBuffer, 0, Math.Min(_tempBuffer.Length, documentSize));
                        if(read == 0)
                            throw new EndOfStreamException();
                        writeBuffer.Write(pTemp, read);
                        documentSize -= read;
                    }
                    
                    _replicatedDocs.Add(curDoc);
                }
            }
            byte* ptr;
            int _;
        }

        public void Dispose()
        {
            _cts.Cancel();
            try
            {
                _stream.Dispose();
            }
            catch (Exception)
            {
            }
            try
            {
                _tcpClient.Dispose();
            }
            catch (Exception)
            {
            }

            if (_incomingThread != Thread.CurrentThread)
            {
                _incomingThread?.Join();
            }
            _incomingThread = null;
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);
    }
}
