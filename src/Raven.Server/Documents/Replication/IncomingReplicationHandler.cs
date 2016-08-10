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
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

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

            _unmanagedWriteBuffer = _context.GetStream();

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
                        using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
                        {
                            //note: at this point, the valid messages are heartbeat and replication batch.
                            _cts.Token.ThrowIfCancellationRequested();

                            long lastReceivedEtag;
                            if (message.TryGet("LastEtag", out lastReceivedEtag))
                                throw new InvalidDataException("The property 'LastEtag' wasn't found in replication batch, invalid data");

                            bool _;
                            if (message.TryGet("Heartbeat", out _))
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Got heartbeat from ({FromToString}) with etag {lastReceivedEtag}.");
                                if (prevRecievedEtag != lastReceivedEtag)
                                {
                                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId, lastReceivedEtag);
                                    prevRecievedEtag = lastReceivedEtag;
                                }
                                SendStatusToSource(writer, -1);
                                continue;
                            }

                            int replicatedDocs = ValidateReplicationBatchAndGetDocsCount(message);

                            try
                            {
                                var sw = Stopwatch.StartNew();
                                using (_context.OpenWriteTransaction())
                                {
                                    ReceiveDocuments(replicatedDocs);

                                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId, lastReceivedEtag);
                                    prevRecievedEtag = lastReceivedEtag;
                                    _context.Transaction.Commit();
                                }
                                sw.Stop();

                                if (_log.IsInfoEnabled)
                                    _log.Info($"Replication connection {FromToString}: received and written {replicatedDocs:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastReceivedEtag}.");

                                //return positive ack

                                SendStatusToSource(writer, lastReceivedEtag);

                                OnDocumentsReceived(this);
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

                                    e.Data.Add("FailedToWrite", true);

                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Failed replicating documents from {FromToString}.",e);
                                    throw;
                                }
                            }
                            finally
                            {
                                try
                                {
                                    writer.Flush();
                                }
                                catch (Exception e)
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Replication connection {FromToString}: failed to send back acknowledgement message for the replication batch. Error thrown : {e}");
                                    // nothing to do at this point
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
                    //if FailedToWrite is in e.Data, we logged the exception already
                    if (_log.IsInfoEnabled && !e.Data.Contains("FailedToWrite"))
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.",e );

                    OnFailed(e, this);
                }
            }
            finally
            {
                _context = null;
                _stream = null;
            }
        }

        private void SendStatusToSource(BlittableJsonTextWriter writer, long lastReceivedEtag)
        {
            var changeVector = new DynamicJsonArray();
            var databaseChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(_context);
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
        private readonly UnmanagedWriteBuffer _unmanagedWriteBuffer;
        private unsafe void ReceiveDocuments(int replicatedDocs)
        {
            var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
            foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(_context))
            {
                maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            }

            fixed (byte* pTemp = _tempBuffer)
            {
                for (int x = 0; x < replicatedDocs; x++)
                {
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var changeVectorEntriesCount = *(int*) pTemp;
                    if (_tempReplicatedChangeVector.Length != changeVectorEntriesCount)
                    {
                        _tempReplicatedChangeVector = new ChangeVectorEntry[changeVectorEntriesCount];
                    }
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(ChangeVectorEntry) * changeVectorEntriesCount);
                    for (int i = 0; i < changeVectorEntriesCount; i++)
                    {
                        _tempReplicatedChangeVector[i] = ((ChangeVectorEntry*)pTemp)[i];
                        long etag;
                        if (maxReceivedChangeVectorByDatabase.TryGetValue(_tempReplicatedChangeVector[i].DbId, out etag) == false ||
                            etag > _tempReplicatedChangeVector[i].Etag)
                        {
                                maxReceivedChangeVectorByDatabase[_tempReplicatedChangeVector[i].DbId] = etag;
                        }
                    }
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var documentSize = *(int*)pTemp;
                    _unmanagedWriteBuffer.Clear();
                    while (documentSize>0)
                    {
                        var read = _multiDocumentParser.Read(_tempBuffer, 0, Math.Min(_tempBuffer.Length, documentSize));
                        if(read == 0)
                            throw new EndOfStreamException();
                        _unmanagedWriteBuffer.Write(pTemp, read);
                        documentSize -= read;
                    }
                    byte* ptr;
                    _unmanagedWriteBuffer.EnsureSingleChunk(out ptr, out documentSize);
                    var doc = new BlittableJsonReaderObject(ptr, documentSize, _context);
                    var id = doc.GetIdFromMetadata();
                    if (id == null)
                        throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");
                    //TODO: conflict handling
                    _database.DocumentsStorage.Put(_context, id, null, doc, _tempReplicatedChangeVector);
                }
            }

            _database.DocumentsStorage.SetDatabaseChangeVector(_context, _database.DocumentsStorage.GetDatabaseChangeVector(_context));
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
