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

        //TODO : do not forget to add logging and code to record stats
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
                                    _log.Info($"Incoming replication thread ({FromToString}) received heartbeat.");
                                if (prevRecievedEtag != lastReceivedEtag)
                                {
                                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId, lastReceivedEtag);
                                    prevRecievedEtag = lastReceivedEtag;
                                }
                                SendStatusToSource(writer, -1);
                                continue;
                            }

                            ThrowIfNotReplicationBatch(message);

                           
                            BlittableJsonReaderArray replicatedDocs;
                            if (!message.TryGet("ReplicationBatch", out replicatedDocs))
                                throw new InvalidDataException(
                                    "Expected the message to have a field with replicated document array, named ReplicationBatch. The property wasn\'t found");

                            try
                            {
                                var sw = Stopwatch.StartNew();
                                using (_context.OpenWriteTransaction())
                                {
                                    ReceiveDocuments(_context, replicatedDocs);
                                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId, lastReceivedEtag);
                                    prevRecievedEtag = lastReceivedEtag;
                                    _context.Transaction.Commit();
                                }
                                sw.Stop();

                                if (_log.IsInfoEnabled)
                                    _log.Info($"Replication connection {FromToString}: received and written {replicatedDocs.Length} documents to database in {sw.ElapsedMilliseconds} ms, with last etag = {lastReceivedEtag}.");

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
                                        _log.Info($"Replication connection {FromToString}: failed writing documents to database - unhandled exception was thrown.{Environment.NewLine} {e}");
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
                        _log.Info($"Replication connection {FromToString}: an exception was thrown during receiving incoming document replication batch. {e}");

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

        private static void ThrowIfNotReplicationBatch(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

            if (!messageType.Equals("ReplicationBatch", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Expected the message 'Type = ReplicationBatch' field, but has 'Type={messageType}'. This is likely a bug.");
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private void ReceiveDocuments(DocumentsOperationContext context, BlittableJsonReaderArray docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
            var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
            foreach (BlittableJsonReaderObject doc in docs)
            {
                var changeVector = doc.EnumerateChangeVector();
                foreach (var currentEntry in changeVector)
                {
                    if (currentEntry.DbId != Guid.Empty) //should never happen, but..
                        throw new InvalidOperationException("change vector database Id is Guid.Empty. This is not supposed to happen and it is likely a bug.");

                    //note: documents in a replication batch are ordered in incremental etag order
                    maxReceivedChangeVectorByDatabase[currentEntry.DbId] = currentEntry.Etag;
                }

                //since blittable deals with offsets, if we want to deserialize embedded object properly,
                //we need to create a new document with proper offsets (that would actually point to embedded object data)
                using (var detachedDoc = context.ReadObject(doc, "IncomingDocumentReplication -> Detach object from parent array"))
                    WriteReceivedDocument(context, detachedDoc);
            }

            dbChangeVector.UpdateChangeVectorFrom(maxReceivedChangeVectorByDatabase);
            _database.DocumentsStorage.SetDatabaseChangeVector(context, dbChangeVector);
        }

        private void WriteReceivedDocument(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {

            var id = doc.GetIdFromMetadata();
            if (id == null)
                throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

            // we need to split this document to an independent blittable document
            // and this time, we'll prepare it for disk.
            doc.PrepareForStorage();
            _database.DocumentsStorage.Put(context, id, null, doc);
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
