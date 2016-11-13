using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Text;
using Raven.Abstractions.Replication;
using Raven.Client.Indexing;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow;
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
        public event Action<IncomingReplicationHandler> IndexesAndTransformersReceived;

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
                            string messageType = null;
                            try
                            {
                                messageType = ValidateReplicationMessageAndGetType(message);

                                switch (messageType)
                                {
                                    case ReplicationMessageType.Documents:
                                        long lastDocumentEtag;
                                        if (!message.TryGet("LastDocumentEtag", out lastDocumentEtag))
                                            throw new InvalidOperationException(
                                                "Expected LastDocumentEtag property in the replication message, but didn't find it..");

                                        HandleReceivedDocumentBatch(message, lastDocumentEtag);
                                        SendStatusToSource(writer, lastDocumentEtag, messageType);
                                        break;
                                    case ReplicationMessageType.IndexesTransformers:
                                        long lastIndexOrTransformerEtag;
                                        if (!message.TryGet("LastIndexOrTransformerEtag", out lastIndexOrTransformerEtag))
                                            throw new InvalidOperationException(
                                                "Expected LastIndexOrTransformerEtag property in the replication message, but didn't find it..");

                                        HandleReceivedIndexOrTransformerBatch(message, lastIndexOrTransformerEtag);
                                        SendStatusToSource(writer, lastIndexOrTransformerEtag, messageType);
                                        break;
                                    case ReplicationMessageType.Heartbeat:
                                        //heartbeat, nothing to do
                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException();
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
                                        ["MessageType"] = messageType,
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

        private long HandleReceivedIndexOrTransformerBatch(BlittableJsonReaderObject message, long lastIndexOrTransformerEtag)
        {
            var replicatedIndexTransformerCount = GetReplicationBatchItemCount(message);

            if (replicatedIndexTransformerCount > 0)
            {
                ReceiveSingleIndexAndTransformersBatch(replicatedIndexTransformerCount, lastIndexOrTransformerEtag);
                OnIndexesAndTransformersReceived(this);
            }

            return replicatedIndexTransformerCount;
        }      

        private long HandleReceivedDocumentBatch(BlittableJsonReaderObject message, long lastDocumentEtag)
        {
            var replicatedDocsCount = GetReplicationBatchItemCount(message);
            ReceiveSingleDocumentsBatch(replicatedDocsCount, lastDocumentEtag);
            OnDocumentsReceived(this);

            return lastDocumentEtag;
        }

        private static int GetReplicationBatchItemCount(BlittableJsonReaderObject message)
        {
            int itemCount;
            if (!message.TryGet("ItemCount", out itemCount))
                throw new InvalidDataException("Expected the 'ItemCount' field, but had no numeric field of this value, this is likely a bug");
            return itemCount;
        }

        private unsafe void ReceiveSingleIndexAndTransformersBatch(int itemCount, long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            var writeBuffer = _context.GetStream();
            // this will read the documents to memory from the network
            // without holding the write tx open
            ReadIndexesTransformersFromSource(ref writeBuffer, itemCount);
            byte* buffer;
            int totalSize;
            writeBuffer.EnsureSingleChunk(out buffer, out totalSize);

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Replication connection {FromToString}: received {itemCount:#,#;;0} indexes and transformers with size {totalSize/1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

            try
            {
                using (_context.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.IndexMetadataPersistence.GetGlobalChangeVector())
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;

                    foreach (var item in _replicatedIndexesAndTransformers)
                    {
                        var relevantMetadata = _database.IndexMetadataPersistence.GetIndexMetadataByName(item.Name);
                        var local = relevantMetadata?.ChangeVector;

                        ReadChangeVector(item, maxReceivedChangeVectorByDatabase);

                        if (local != null)
                            //if local == null --> this is a tombstone and thus incoming item should be accepted
                        {
                            //do not accept incoming index/transformer if it's change vector is lower
                            //--> change vector is lower if at least one local change vector etag is higher than the one in incoming
                            if (ShouldSkipIndexOrTransformer(local, maxReceivedChangeVectorByDatabase))
                            {
                                LogSkippedIndexOrTransformer(item, maxReceivedChangeVectorByDatabase, local);
                                continue;
                            }
                        }


                        //TODO : find a good way to get build version, 
                        //so this will remain compatible between different 4.x versions
                        const int BuildVersion = 40000;

                        using (
                            var definition = new BlittableJsonReaderObject(buffer + item.Position, item.DefinitionSize,
                                _context))
                        {
                            switch (item.Type)
                            {
                                case IndexEntryType.Index:
                                    if (_log.IsInfoEnabled)
                                    {
                                        _log.Info($"Replicated index with name = {item.Name}");
                                    }
                                    _database.IndexStore.TryDeleteIndexIfExists(item.Name);
                                    try
                                    {
                                        IndexProcessor.Import(definition, _database, BuildVersion);
                                    }
                                    catch (ArgumentException e)
                                    {
                                        if (_log.IsOperationsEnabled)
                                            _log.Operations(
                                                $"Failed to read index (name = {item.Name}, etag = {item.Etag}) definition from incoming replication batch. This is not supposed to happen.",
                                                e);
                                        throw;
                                    }
                                    break;
                                case IndexEntryType.Transformer:
                                    if (_log.IsInfoEnabled)
                                    {
                                        _log.Info($"Replicated tranhsformer with name = {item.Name}");
                                    }

                                    _database.TransformerStore.DeleteTransformer(item.Name);

                                    try
                                    {
                                        TransformerProcessor.Import(definition, _database, BuildVersion);
                                    }
                                    catch (ArgumentException e)
                                    {
                                        if (_log.IsOperationsEnabled)
                                            _log.Operations(
                                                $"Failed to read transformer (name = {item.Name}, etag = {item.Etag}) definition from incoming replication batch. This is not supposed to happen.",
                                                e);
                                        throw;
                                    }
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }
                        }
                        _database.IndexMetadataPersistence.SetGlobalChangeVector(maxReceivedChangeVectorByDatabase);
                        _database.DocumentsStorage.SetLastReplicateEtagFrom(_context,
                            ConnectionInfo.SourceDatabaseId, lastEtag);
                    }

                    _context.Transaction.Commit();
                }
            }
            finally
            {
                writeBuffer.Dispose();
            }
        }

        private void LogSkippedIndexOrTransformer(
            ReplicationIndexOrTransformerPositions item, 
            Dictionary<Guid, long> maxReceivedChangeVectorByDatabase, 
            ChangeVectorEntry[] local)
        {
            string whatIsIt = null;
            switch (item.Type)
            {
                case IndexEntryType.Index:
                    whatIsIt = "index";
                    break;
                case IndexEntryType.Transformer:
                    whatIsIt = "transformer";
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            var msg =
                $"Received {whatIsIt} via replication from {_replicationDocument.Source} ( {whatIsIt} name = {item.Name}), but it's change vector was smaller than the local, so I skipped it. Remote change vector = {ReplicationUtils.ChangeVectorToString(maxReceivedChangeVectorByDatabase)}, Local change vector = {ReplicationUtils.ChangeVectorToString(local)}";
            if (_log.IsOperationsEnabled)
            {
                //this is severe enough to warrant 'operations' log entry
                _log.Operations(msg);
            }
            _database.Alerts.AddAlert(new Alert
            {
                Key = _replicationDocument.Source,
                Type = AlertType.Replication,
                Message = msg,
                CreatedAt = DateTime.UtcNow,
                Severity = AlertSeverity.Warning
            });
        }

        private static bool ShouldSkipIndexOrTransformer(ChangeVectorEntry[] local, Dictionary<Guid, long> remote)
        {
            var remoteHasAnySmaller = false;
            var remoteHasAnyLarger = false;
            for (int index = 0; index < local.Length; index++)
            {
                var cv = local[index];
                long etag;
                if (remote.TryGetValue(cv.DbId, out etag))
                {
                    if (etag < cv.Etag)
                    {
                        remoteHasAnySmaller = true;
                        break;
                    }

                    if (etag >= cv.Etag)
                        remoteHasAnyLarger = true;
                }
            }

            //if change vectors are conflicted, we proceed anyway
            return remoteHasAnySmaller && !remoteHasAnyLarger;
        }

        private unsafe void ReadIndexesTransformersFromSource(ref UnmanagedWriteBuffer writeBuffer, int itemCount)
        {
            _replicatedIndexesAndTransformers.Clear();
            fixed (byte* pTemp = _tempBuffer)
            {
                for (int x = 0; x < itemCount; x++)
                {
                    var curItem = new ReplicationIndexOrTransformerPositions
                    {
                        Position = writeBuffer.SizeInBytes
                    };

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var changeVectorCount = *(int*)pTemp;

                    var changeVectorSize = sizeof(ChangeVectorEntry) * changeVectorCount;
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, changeVectorSize);
                    curItem.ChangeVector = new ChangeVectorEntry[changeVectorCount];
                    fixed (ChangeVectorEntry* pChangeVector = curItem.ChangeVector)
                        Memory.Copy((byte*) pChangeVector, pTemp, changeVectorSize);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(long));
                    curItem.Etag = *(long*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    int typeAsInt = *(int*) pTemp;
                    curItem.Type = (IndexEntryType)typeAsInt;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var nameSize = *(int*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var charCount = *(int*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, nameSize);
                    curItem.Name = new string(' ',charCount);
                    fixed (char* pName = curItem.Name)
                        Encoding.UTF8.GetChars(pTemp, nameSize, pName, charCount);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var definitionSize = curItem.DefinitionSize = *(int*)pTemp;

                    while (definitionSize > 0)
                    {
                        var toRead = Math.Min(_tempBuffer.Length, definitionSize);

                        var read = _multiDocumentParser.Read(_tempBuffer, 0, toRead);
                        if (read == 0)
                            throw new EndOfStreamException();
                        writeBuffer.Write(pTemp, read);
                        definitionSize -= read;
                    }

                    _replicatedIndexesAndTransformers.Add(curItem);
                }
            }
        }

        private unsafe void ReceiveSingleDocumentsBatch(int replicatedDocsCount, long lastEtag)
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


        private void ReadChangeVector(ReplicationIndexOrTransformerPositions index, Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            for (int i = 0; i < index.ChangeVector.Length; i++)
            {
                long etag;
                if (maxReceivedChangeVectorByDatabase.TryGetValue(index.ChangeVector[i].DbId, out etag) == false ||
                    etag > index.ChangeVector[i].Etag)
                {
                    maxReceivedChangeVectorByDatabase[index.ChangeVector[i].DbId] = index.ChangeVector[i].Etag;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string ValidateReplicationMessageAndGetType(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");            

            return messageType;
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        public ReplicationDocument ReplicationDocument => 
            _replicationDocument ?? (_replicationDocument = _parent.GetReplicationDocument());

        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>();
        private readonly List<ReplicationIndexOrTransformerPositions> _replicatedIndexesAndTransformers = new List<ReplicationIndexOrTransformerPositions>();

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public int DocumentSize;
            public string Collection;
        }

        public struct ReplicationIndexOrTransformerPositions
        {
            public string Id;
            public int Position;
            public ChangeVectorEntry[] ChangeVector;

            public int DefinitionSize;
            public int DefinitionCharCount;
            public BlittableJsonReaderObject Definition;
            public long Etag;
            public IndexEntryType Type;
            public string Name;
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
        protected void OnIndexesAndTransformersReceived(IncomingReplicationHandler instance) => IndexesAndTransformersReceived?.Invoke(instance);        

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
