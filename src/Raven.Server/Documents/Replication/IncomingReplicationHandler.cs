using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Text;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private readonly NetworkStream _stream;
        private readonly DocumentReplicationLoader _parent;
        private readonly DocumentsOperationContext _context;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        private readonly IDisposable _contextDisposable;
        private ReplicationDocument _replicationDocument;
        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;

        public IncomingReplicationHandler(
            JsonOperationContext.MultiDocumentParser multiDocumentParser, 
            DocumentDatabase database, 
            TcpClient tcpClient, 
            NetworkStream stream, 
            ReplicationLatestEtagRequest replicatedLastEtag,
            DocumentReplicationLoader parent)
        {
            
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            _multiDocumentParser = multiDocumentParser;
            _database = database;
            _tcpClient = tcpClient;
            _stream = stream;
            _parent = parent;
            _contextDisposable = _database.DocumentsStorage
                                          .ContextPool
                                          .AllocateOperationContext(out _context);

            _log = LoggingSource.Instance.GetLogger<IncomingReplicationHandler>(_database.Name);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _incomingThread = new Thread(ReceiveReplationBatches)
            {
                IsBackground = true,
                Name = $"Incoming replication {FromToString}"
            };
            _incomingThread.Start();
            if (_log.IsInfoEnabled)
                _log.Info($"Incoming replication thread started ({FromToString})");

        }

        [ThreadStatic]
        public static bool IsIncomingReplicationThread;

        private void ReceiveReplationBatches()
        {
            IsIncomingReplicationThread = true;

            bool exceptionLogged = false;
            try
            {
                using (_contextDisposable)
                using (_stream)
                using (var writer = new BlittableJsonTextWriter(_context, _stream))
                using (_multiDocumentParser)
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        _context.ResetAndRenew();

                        using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
                        {
                            //note: at this point, the valid messages are heartbeat and replication batch.
                            _cts.Token.ThrowIfCancellationRequested();

                            try
                            {
                                ValidateReplicationBatchAndGetDocsCount(message);

                                long lastDocumentEtag;
                                long lastIndexOrTransformerEtag;                                

                                if (message.TryGet("LastDocumentEtag", out lastDocumentEtag))
                                {
                                    HandleReceivedDocumentBatch(message, lastDocumentEtag);

                                    //return positive ack
                                    SendStatusToSource(writer, lastDocumentEtag,"Documents");
                                }
                                else if (message.TryGet("LastIndexOrTransformerEtag", out lastIndexOrTransformerEtag) == false)
                                {
                                    HandleReceivedIndexOrTransformerBatch(message, lastIndexOrTransformerEtag);

                                    //return positive ack
                                    SendStatusToSource(writer, lastIndexOrTransformerEtag, "IndexOrTransformers");
                                }
                                else
                                {
                                    throw new InvalidDataException(
                                        "The property 'LastDocumentEtag' and 'LastIndexOrTransformerEtag' weren't found in replication batch, invalid format");
                                }

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
                                        _log.Info($"Failed replicating documents from {FromToString}.", e);
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
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.", e);

                    OnFailed(e, this);
                }
            }
        }

        private void HandleReceivedIndexOrTransformerBatch(BlittableJsonReaderObject message, long lastIndexOrTransformerEtag)
        {
            throw new NotImplementedException();
        }

        private long HandleReceivedDocumentBatch(BlittableJsonReaderObject message, long lastDocumentEtag)
        {
            int replicatedDocsCount;
            if (!message.TryGet("Documents", out replicatedDocsCount))
                throw new InvalidDataException(
                    $"Expected the 'Documents' field, but had no numeric field of this value, this is likely a bug");

            //replicatedDocsCount == 0 --> this is heartbeat message
            if (replicatedDocsCount > 0)
            {
                ReceiveSingleBatch(replicatedDocsCount, lastDocumentEtag);
                OnDocumentsReceived(this);
            }

            return lastDocumentEtag;
        }

        private unsafe void ReceiveSingleBatch(int replicatedDocsCount, long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            var writeBuffer = _context.GetStream();
            try
            {
                // this will read the documents to memory from the network
                // without holding the write tx open
                ReadDocumentsFromSource(ref writeBuffer, replicatedDocsCount);
                byte* buffer;
                int totalSize;
                writeBuffer.EnsureSingleChunk(out buffer, out totalSize);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {replicatedDocsCount:#,#;;0} documents with size {totalSize/1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (_context.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(_context))
                    {
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }

                    foreach (var doc in _replicatedDocs)
                    {
                        ReadChangeVector(doc, buffer, maxReceivedChangeVectorByDatabase);

                        BlittableJsonReaderObject json = null;
                        if (doc.DocumentSize >= 0) //no need to load document data for tombstones
                            // document size == -1 --> doc is a tombstone
                        {
                            if (doc.Position + doc.DocumentSize > totalSize)
                                ThrowInvalidSize(totalSize, doc);

                            //if something throws at this point, this means something is really wrong and we should stop receiving documents.
                            //the other side will receive negative ack and will retry sending again.
                            json = new BlittableJsonReaderObject(
                                buffer + doc.Position + (doc.ChangeVectorCount*sizeof(ChangeVectorEntry)),
                                doc.DocumentSize, _context);
                        }
                        ChangeVectorEntry[] conflictingVector;
                        var conflictStatus = GetConflictStatus(_context, doc.Id, _tempReplicatedChangeVector,out conflictingVector);
                        switch (conflictStatus)
                        {
                            case ConflictStatus.Update:
                                if (json != null)
                                {
                                    _database.DocumentsStorage.Put(_context, doc.Id, null, json, _tempReplicatedChangeVector);
                                }
                                else
                                {									
                                    _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                                        _context,doc.Id,										
                                        _tempReplicatedChangeVector,
                                        doc.Collection);
                                }
                                break;
                            case ConflictStatus.ShouldResolveConflict:
                                _context.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_context, doc.Id);
                                goto case ConflictStatus.Update;
                            case ConflictStatus.Conflict:
                                HandleConflict(doc, conflictingVector, json);
                                break;
                            case ConflictStatus.AlreadyMerged:
                                //nothing to do
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(conflictStatus), "Invalid ConflictStatus: " + conflictStatus);
                        }
                    }
                    _database.DocumentsStorage.SetDatabaseChangeVector(_context,
                        maxReceivedChangeVectorByDatabase);

                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId,
                        lastEtag);
                    _context.Transaction.Commit();
                }
                sw.Stop();

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received and written {replicatedDocsCount:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastEtag}.");
            }
            finally
            {
                writeBuffer.Dispose();
            }
        }

        private static void ThrowInvalidSize(int totalSize, ReplicationDocumentsPositions doc)
        {
            throw new ArgumentOutOfRangeException(
                $"Reading past the size of buffer! TotalSize {totalSize} but position is {doc.Position} & size is {doc.DocumentSize}!");
        }


        private void HandleConflict(
            ReplicationDocumentsPositions docPosition, 
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {
            switch (ReplicationDocument?.DocumentConflictResolution ?? StraightforwardConflictResolution.None)
            {
                case StraightforwardConflictResolution.ResolveToLocal:
                    RespolveConflictToLocal(docPosition, conflictingVector);
                    break;
                case StraightforwardConflictResolution.ResolveToRemote:
                    RespolveConflictToRemote(docPosition, doc, conflictingVector);
                    break;
                case StraightforwardConflictResolution.ResolveToLatest:
                    if (conflictingVector == null) //precaution
                    {
                        throw new InvalidOperationException(
                            "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");
                    }
                    
                    DateTime localLastModified;
                    var relevantLocalConflict = _context.DocumentDatabase.DocumentsStorage.GetConflictForChangeVector(_context, docPosition.Id, conflictingVector);
                    if (relevantLocalConflict != null)
                    {
                        localLastModified = relevantLocalConflict.Doc.GetLastModified();
                    }
                    else //the conflict is with existing document/tombstone
                    {
                        var relevantLocalDoc = _context.DocumentDatabase.DocumentsStorage
                            .GetDocumentOrTombstone(
                                _context,
                                docPosition.Id);
                        if(relevantLocalDoc.Item1 != null)
                            localLastModified = relevantLocalDoc.Item1.Data.GetLastModified();
                        else if (relevantLocalDoc.Item2 != null)
                        {
                            RespolveConflictToRemote(docPosition, doc, conflictingVector);
                            return;
                        }
                        else //precaution, not supposed to get here
                        {
                            throw new InvalidOperationException(
                                $"Didn\'t find document neither tombstone for specified id ({docPosition.Id}), this is not supposed to happen and is likely a bug.");
                        }
                    }
                    var remoteLastModified = doc.GetLastModified();
                    if (remoteLastModified > localLastModified)
                    {
                        RespolveConflictToRemote(docPosition, doc, conflictingVector);
                    }
                    else
                    {
                        RespolveConflictToLocal(docPosition, conflictingVector);
                    }
                    break;
                default:
                    _database.DocumentsStorage.AddConflict(_context, docPosition.Id, doc, _tempReplicatedChangeVector);
                    break;
            }
        }

        

        private void RespolveConflictToRemote(ReplicationDocumentsPositions doc, 
            BlittableJsonReaderObject json, 
            ChangeVectorEntry[] conflictingVector)
        {
            var merged = ReplicationUtils.MergeVectors(conflictingVector, _tempReplicatedChangeVector);
            _context.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_context, doc.Id);
            _database.DocumentsStorage.Put(_context, doc.Id, null, json, merged);
        }

        private void RespolveConflictToLocal(ReplicationDocumentsPositions doc, ChangeVectorEntry[] conflictingVector)
        {
            var relevantLocalConflict =
                _context.DocumentDatabase.DocumentsStorage.GetConflictForChangeVector(
                    _context,
                    doc.Id,
                    conflictingVector);

            var merged = ReplicationUtils.MergeVectors(conflictingVector, _tempReplicatedChangeVector);

            //if we reached the state of conflict, there must be at least one conflicting change vector locally
            //thus, should not happen
            if (relevantLocalConflict != null)
            {
                _context.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_context, doc.Id);

                if (relevantLocalConflict.Doc != null)
                {
                    _database.DocumentsStorage.Put(
                        _context,
                        doc.Id,
                        null,
                        relevantLocalConflict.Doc,
                        merged);
                }
                else //resolving to tombstone
                {
                    _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                        _context,
                        doc.Id,
                        merged,
                        doc.Collection);
                }
            }
        }


        private unsafe void ReadChangeVector(ReplicationDocumentsPositions doc, byte* buffer,
            Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            if (_tempReplicatedChangeVector.Length != doc.ChangeVectorCount)
            {
                _tempReplicatedChangeVector = new ChangeVectorEntry[doc.ChangeVectorCount];
            }
            for (int i = 0; i < doc.ChangeVectorCount; i++)
            {
                _tempReplicatedChangeVector[i] = ((ChangeVectorEntry*)(buffer + doc.Position))[i];
                long etag;
                if (maxReceivedChangeVectorByDatabase.TryGetValue(_tempReplicatedChangeVector[i].DbId, out etag) == false ||
                    etag > _tempReplicatedChangeVector[i].Etag)
                {
                    maxReceivedChangeVectorByDatabase[_tempReplicatedChangeVector[i].DbId] = _tempReplicatedChangeVector[i].Etag;
                }
            }
        }      

        private void SendStatusToSource(BlittableJsonTextWriter writer, long lastEtag, string handledMessageType)
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
                    ["DbId"] = changeVectorEntry.DbId.ToString(),
                    ["Etag"] = changeVectorEntry.Etag
                });
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"Sending ok => {FromToString} with last etag {lastEtag} and change vector: {databaseChangeVector.Format()}");
            }
            _context.Write(writer, new DynamicJsonValue
            {
                ["Type"] = "Ok",
                ["MessageType"] = handledMessageType,
                ["LastEtagAccepted"] = lastEtag,
                ["Error"] = null,
                ["CurrentChangeVector"] = changeVector
            });

            writer.Flush();
        }

        private static void ValidateReplicationBatchAndGetDocsCount(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

            if (!messageType.Equals("ReplicationBatch", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Expected the message 'Type = ReplicationBatch' field, but has 'Type={messageType}'. This is likely a bug.");
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        public ReplicationDocument ReplicationDocument => 
            _replicationDocument ?? (_replicationDocument = _parent.GetReplicationDocument());

        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>();

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public int DocumentSize;
            public string Collection;
        }

        private unsafe void ReadDocumentsFromSource(ref UnmanagedWriteBuffer writeBuffer, int replicatedDocs)
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
                    curDoc.ChangeVectorCount = *(int*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(ChangeVectorEntry) * curDoc.ChangeVectorCount);
                    writeBuffer.Write(_tempBuffer, 0, sizeof(ChangeVectorEntry) * curDoc.ChangeVectorCount);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var keySize = *(int*)pTemp;
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, keySize);
                    curDoc.Id = Encoding.UTF8.GetString(_tempBuffer, 0, keySize);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var documentSize = curDoc.DocumentSize = *(int*)pTemp;
                    if (documentSize != -1) //if -1, then this is a tombstone
                    {
                        while (documentSize > 0)
                        {
                            var read = _multiDocumentParser.Read(_tempBuffer, 0, Math.Min(_tempBuffer.Length, documentSize));
                            if (read == 0)
                                throw new EndOfStreamException();
                            writeBuffer.Write(pTemp, read);
                            documentSize -= read;
                        }
                    }
                    else
                    {
                        //read the collection of the tombstone
                        _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                        var collectionSize = *(int*)pTemp;
                        _multiDocumentParser.ReadExactly(_tempBuffer, 0, collectionSize);
                        curDoc.Collection = Encoding.UTF8.GetString(_tempBuffer, 0, collectionSize);
                    }
                    _replicatedDocs.Add(curDoc);
                }
            }
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

        private ConflictStatus GetConflictStatus(DocumentsOperationContext context, string key, ChangeVectorEntry[] remote,out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = context.DocumentDatabase.DocumentsStorage.GetConflictsFor(context, key);
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    if (GetConflictStatus(remote, existingConflict.ChangeVector) == ConflictStatus.Conflict)
                    {
                        conflictingVector = existingConflict.ChangeVector;
                        return ConflictStatus.Conflict;
                    }
                }

                return ConflictStatus.ShouldResolveConflict;
            }

            var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, key);
            ChangeVectorEntry[] local;

            if (result.Item1 != null)
                local = result.Item1.ChangeVector;
            else if (result.Item2 != null)
                local = result.Item2.ChangeVector;
            else
                return ConflictStatus.Update; //document with 'key' doesnt exist locally, so just do PUT

            
            var status = GetConflictStatus(remote, local);
            if (status == ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }			

            return status;
        }

        public enum ConflictStatus
        {
            Update,
            Conflict,
            AlreadyMerged,
            ShouldResolveConflict
        }

        public static ConflictStatus GetConflictStatus(ChangeVectorEntry[] remote, ChangeVectorEntry[] local)
        {
            //any missing entries from a change vector are assumed to have zero value
            var remoteHasLargerEntries = local.Length < remote.Length;
            var localHasLargerEntries = remote.Length < local.Length;

            int remoteEntriesTakenIntoAccount = 0;
            for (int index = 0; index < local.Length; index++)
            {
                if (remote.Length < index && remote[index].DbId == local[index].DbId)
                {
                    remoteHasLargerEntries |= remote[index].Etag > local[index].Etag;
                    localHasLargerEntries |= local[index].Etag > remote[index].Etag;
                    remoteEntriesTakenIntoAccount++;
                }
                else
                {
                    var updated = false;
                    for (var remoteIndex = 0; remoteIndex < remote.Length; remoteIndex++)
                    {
                        if (remote[remoteIndex].DbId == local[index].DbId)
                        {
                            remoteHasLargerEntries |= remote[remoteIndex].Etag > local[index].Etag;
                            localHasLargerEntries |= local[index].Etag > remote[remoteIndex].Etag;
                            remoteEntriesTakenIntoAccount++;
                            updated = true;
                        }
                    }

                    if (!updated)
                        localHasLargerEntries = true;
                }
            }
            remoteHasLargerEntries |= remoteEntriesTakenIntoAccount < remote.Length;

            if (remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.Conflict;

            if (!remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.AlreadyMerged;

            return ConflictStatus.Update;
        }
    }
}
