using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Text;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Alerts;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Util;
using Raven.Server.Documents.Patch;
using Raven.Server.Exceptions;
using Constants = Raven.Abstractions.Data.Constants;
using ThreadState = System.Threading.ThreadState;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private readonly NetworkStream _stream;
        private readonly DocumentReplicationLoader _parent;
        private readonly DocumentsOperationContext _documentsContext;
        private readonly TransactionOperationContext _configurationContext;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
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

            _disposables.Add(_database.DocumentsStorage.ContextPool
                .AllocateOperationContext(out _documentsContext));

            _disposables.Add(_database.ConfigurationStorage.ContextPool
                .AllocateOperationContext(out _configurationContext));


            _log = LoggingSource.Instance.GetLogger<IncomingReplicationHandler>(_database.Name);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }


        public void Start()
        {
            if (_incomingThread != null)
                return;

            var result = Interlocked.CompareExchange(ref _incomingThread, new Thread(ReceiveReplationBatches)
            {
                IsBackground = true,
                Name = $"Incoming replication {FromToString}"
            }, null);

            if (result != null)
                return; // already set by someone else, they can start it

            if (_incomingThread.ThreadState != ThreadState.Running)
            {
                _incomingThread.Start();

                if (_log.IsInfoEnabled)
                    _log.Info($"Incoming replication thread started ({FromToString})");
            }
        }

        [ThreadStatic]
        public static bool IsIncomingReplicationThread;

        private readonly AsyncManualResetEvent _replicationFromAnotherSource = new AsyncManualResetEvent();

        public void OnReplicationFromAnotherSource()
        {
            _replicationFromAnotherSource.SetByAsyncCompletion();
        }

        private void ReceiveReplationBatches()
        {
            IsIncomingReplicationThread = true;
            try
            {
                using (_stream)
                using (var writer = new BlittableJsonTextWriter(_documentsContext, _stream))
                using (_multiDocumentParser)
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        _documentsContext.ResetAndRenew();
                        _configurationContext.ResetAndRenew();
                        try
                        {
                            using (var msg = _multiDocumentParser.InterruptibleParseToMemory(
                                "IncomingReplication/read-message", _replicationFromAnotherSource))
                            {
                                if (msg != null)
                                {
                                    HandleSingleReplicationBatch(msg, writer);
                                }
                                else // notify about new change vector
                                {
                                    SendHeartbeatStatusToSource(writer, _lastDocumentEtag, _lastIndexOrTransformerEtag, "Notify");
                                }
                                // we reset it after every time we send to the remote server
                                // because that is when we know that it is up to date with our 
                                // status, so no need to send again
                                _replicationFromAnotherSource.Reset();
                            }
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                if(e.InnerException is SocketException)
                                    _log.Info("Failed to read data from incoming connection. The incoming connection will be closed and re-created.", e);
                                else
                                    _log.Info("Received unexpected exception while receiving replication batch. This is not supposed to happen.",e);
                            }

                            throw;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (!_cts.IsCancellationRequested)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.", e);

                    OnFailed(e, this);
                }
            }
        }

        private void HandleSingleReplicationBatch(BlittableJsonReaderObject message, BlittableJsonTextWriter writer)
        {
            message.BlittableValidation();
            //note: at this point, the valid messages are heartbeat and replication batch.
            _cts.Token.ThrowIfCancellationRequested();
            string messageType = null;
            try
            {
                if (!message.TryGet(nameof(ReplicationMessageHeader.Type), out messageType))
                    throw new InvalidDataException(
                        "Expected the message to have a 'Type' field. The property was not found");

                if (!message.TryGet(nameof(ReplicationMessageHeader.LastDocumentEtag), out _lastDocumentEtag))
                    throw new InvalidOperationException(
                        "Expected LastDocumentEtag property in the replication message, but didn't find it..");

                if (
                    !message.TryGet(nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag),
                        out _lastIndexOrTransformerEtag))
                    throw new InvalidOperationException(
                        "Expected LastIndexOrTransformerEtag property in the replication message, but didn't find it..");

                switch (messageType)
                {
                    case ReplicationMessageType.Documents:
                        HandleReceivedDocumentBatch(message, _lastDocumentEtag);
                        break;
                    case ReplicationMessageType.IndexesTransformers:
                        HandleReceivedIndexOrTransformerBatch(message, _lastIndexOrTransformerEtag);
                        break;
                    case ReplicationMessageType.Heartbeat:
                        //nothing to do..
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                SendHeartbeatStatusToSource(writer, _lastDocumentEtag, _lastIndexOrTransformerEtag, messageType);
            }
            catch (ObjectDisposedException)
            {
                //we are shutting down replication, this is ok                                
            }
            catch (EndOfStreamException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        "Received unexpected end of stream while receiving replication batches. This might indicate an issue with network.",
                        e);
                throw;
            }
            catch (Exception e)
            {
                //if we are disposing, ignore errors
                if (!_cts.IsCancellationRequested && !(e is ObjectDisposedException))
                {
                    //return negative ack
                    var returnValue = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = ReplicationMessageReply.ReplyType.Error.ToString(),
                        [nameof(ReplicationMessageReply.MessageType)] = messageType,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = -1,
                        [nameof(ReplicationMessageReply.LastIndexTransformerEtagAccepted)] = -1,
                        [nameof(ReplicationMessageReply.Exception)] = e.ToString()
                    };

                    _documentsContext.Write(writer, returnValue);
                    writer.Flush();

                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed replicating documents from {FromToString}.", e);

                    throw;
                }
            }
        }

        private void HandleReceivedIndexOrTransformerBatch(BlittableJsonReaderObject message, long lastIndexOrTransformerEtag)
        {
            int itemCount;
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemCount), out itemCount))
                throw new InvalidDataException("Expected the 'ItemCount' field, but had no numeric field of this value, this is likely a bug");
            var replicatedIndexTransformerCount = itemCount;

            if (replicatedIndexTransformerCount <= 0)
                return;

            ReceiveSingleIndexAndTransformersBatch(replicatedIndexTransformerCount, lastIndexOrTransformerEtag);
            OnIndexesAndTransformersReceived(this);
        }

        private void HandleReceivedDocumentBatch(BlittableJsonReaderObject message, long lastDocumentEtag)
        {
            int itemCount;
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemCount), out itemCount))
                throw new InvalidDataException("Expected the 'ItemCount' field, but had no numeric field of this value, this is likely a bug");

            ReceiveSingleDocumentsBatch(itemCount, lastDocumentEtag);
            OnDocumentsReceived(this);
        }

        private unsafe void ReceiveSingleIndexAndTransformersBatch(int itemCount, long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            var writeBuffer = _configurationContext.GetStream();
            // this will read the indexes to memory from the network
            try
            {
                ReadIndexesTransformersFromSource(ref writeBuffer, itemCount);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        "Failed to read transformer information from replication message. This is not supposed to happen and it is likely due to a bug.",
                        e);
                throw;
            }

            try
            {
                byte* buffer;
                int totalSize;
                writeBuffer.EnsureSingleChunk(out buffer, out totalSize);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {itemCount:#,#;;0} indexes and transformers with size {totalSize/1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");
                var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                using (var tx = _configurationContext.OpenReadTransaction())
                {
                    foreach (
                        var changeVectorEntry in
                        _database.IndexMetadataPersistence.GetIndexesAndTransformersChangeVector(tx.InnerTransaction))
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                }

                foreach (var item in _replicatedIndexesAndTransformers)
                {
                    var remote = item.ChangeVector;

                    ChangeVectorEntry[] conflictingVector;
                    ConflictStatus conflictStatus;
                    using (_configurationContext.OpenReadTransaction())
                        conflictStatus = GetConflictStatusForIndexOrTransformer(_configurationContext, item.Name,
                            remote,
                            out conflictingVector);

                    ReadChangeVector(item, maxReceivedChangeVectorByDatabase);

                    using (var definition = new BlittableJsonReaderObject(buffer + item.Position, item.DefinitionSize,_configurationContext))
                    {
                        switch (conflictStatus)
                        {
                            case ConflictStatus.ShouldResolveConflict:
                            //note : PutIndexOrTransformer() is deleting conflicts and merges chnage vectors
                            //of the conflicts. This can be seen in IndexesEtagsStorage::WriteEntry()
                            case ConflictStatus.Update:                                
                                PutIndexOrTransformer(item, definition);
                                break;
                            case ConflictStatus.Conflict:
                                using (var txw = _configurationContext.OpenWriteTransaction())
                                {
                                    HandleConflictForIndexOrTransformer(item, definition, conflictingVector, txw, _configurationContext);

                                    UpdateIndexesChangeVector(txw, lastEtag, maxReceivedChangeVectorByDatabase);

                                    txw.Commit();
                                    return; // skip the UpdateIndexesChangeVector below to avoid duplicate calls
                                }
                            case ConflictStatus.AlreadyMerged:
                                if (_log.IsInfoEnabled)
                                    _log.Info(
                                        $"Conflict check resolved to AlreadyMerged operation, nothing to do for index = {item.Name}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                //nothing to do...
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(conflictStatus),
                                    "Invalid ConflictStatus: " + conflictStatus);
                        }

                        using (var txw = _configurationContext.OpenWriteTransaction())
                        {
                            UpdateIndexesChangeVector(txw, lastEtag, maxReceivedChangeVectorByDatabase);

                            txw.Commit();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Failed to receive index/transformer replication batch. This is not supposed to happen, and is likely a bug.", e);
                throw;
            }
            finally
            {
                _replicatedIndexesAndTransformers.Clear();
                writeBuffer.Dispose();
            }
        }

        private void UpdateIndexesChangeVector(RavenTransaction txw, long lastEtag, Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            _database.IndexMetadataPersistence.SetGlobalChangeVector(txw.InnerTransaction,
                _documentsContext.Allocator, maxReceivedChangeVectorByDatabase);
            _database.IndexMetadataPersistence.SetLastReplicateEtagFrom(txw.InnerTransaction,
                _documentsContext.Allocator, ConnectionInfo.SourceDatabaseId, lastEtag);
        }

        private void HandleConflictForIndexOrTransformer(ReplicationIndexOrTransformerPositions item, BlittableJsonReaderObject definition, ChangeVectorEntry[] conflictingVector, RavenTransaction tx, TransactionOperationContext context)
        {            
            switch (item.Type)
            {
                case IndexEntryType.Index:
                case IndexEntryType.Transformer:
                    var replicationSource = $"{ConnectionInfo.SourceUrl} --> {ConnectionInfo.SourceDatabaseId}";
                    var msg = $"Received {item.Type} via replication from {replicationSource} ( {item.Type} name = {item.Name}), with change vector {conflictingVector.Format()}, and it is conflicting";
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(msg);
                    }

                    _database.IndexMetadataPersistence.AddConflict(_configurationContext, _configurationContext.Transaction.InnerTransaction,
                        item.Name, item.Type, conflictingVector, definition);

                    //this is severe enough to warrant an alert
                    _database.Alerts.AddAlert(new Alert
                    {
                        Key = replicationSource,
                        Type = AlertType.Replication,
                        Message = msg,
                        CreatedAt = DateTime.UtcNow,
                        Severity = AlertSeverity.Warning
                    },context,tx);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PutIndexOrTransformer(ReplicationIndexOrTransformerPositions item, BlittableJsonReaderObject definition)
        {
            switch (item.Type)
            {
                case IndexEntryType.Index:
                    PutIndexReplicationItem(item, definition);
                    break;
                case IndexEntryType.Transformer:
                    PutTransformerReplicationItem(item, definition);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private void PutTransformerReplicationItem(ReplicationIndexOrTransformerPositions item, BlittableJsonReaderObject definition)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Replicated tranhsformer with name = {item.Name}");
            }

            _database.TransformerStore.TryDeleteTransformerIfExists(item.Name);

            try
            {
                TransformerProcessor.Import(definition, _database, ServerVersion.Build);
            }
            catch (ArgumentException e)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations(
                        $"Failed to read transformer (name = {item.Name}, etag = {item.Etag}) definition from incoming replication batch. This is not supposed to happen.",
                        e);
                throw;
            }
        }

        private void PutIndexReplicationItem(ReplicationIndexOrTransformerPositions item, BlittableJsonReaderObject definition)
        {
            var existing = _database.IndexStore.GetIndex(item.Name);

            if (existing != null)
            {
                using (var existingDefinition = _documentsContext.ReadObject(existing.GetIndexDefinition().ToJson(),
                    "Replication/Index/read existing index definition"))
                {
                    if (definition.Equals(existingDefinition))
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info(
                                $"Replicated index with name = {item.Name}, but there is already existing index with the same definition. Skipping index PUT.");
                        }

                        return;
                    }
                }
            }

            if (_log.IsInfoEnabled)
            {
                _log.Info($"Replicated index with name = {item.Name}");
            }

            _database.IndexStore.TryDeleteIndexIfExists(item.Name);
            try
            {
                IndexProcessor.Import(definition, _database, ServerVersion.Build, false);               
            }
            catch (ArgumentException e)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations(
                        $"Failed to read index (name = {item.Name}, etag = {item.Etag}) definition from incoming replication batch. This is not supposed to happen.",
                        e);
                throw;
            }
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
                        Memory.Copy((byte*)pChangeVector, pTemp, changeVectorSize);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(long));
                    curItem.Etag = *(long*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    int typeAsInt = *(int*)pTemp;
                    curItem.Type = (IndexEntryType)typeAsInt;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var nameSize = *(int*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var charCount = *(int*)pTemp;

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, nameSize);
                    curItem.Name = new string(' ', charCount);
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
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Receiving replication batch with {replicatedDocsCount} documents starting with {lastEtag} from {ConnectionInfo}");
            }

            var sw = Stopwatch.StartNew();
            var writeBuffer = _documentsContext.GetStream();
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
                        $"Replication connection {FromToString}: received {replicatedDocsCount:#,#;;0} documents with size {totalSize / 1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (_documentsContext.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(_documentsContext))
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
                                buffer + doc.Position + (doc.ChangeVectorCount * sizeof(ChangeVectorEntry)),
                                doc.DocumentSize, _documentsContext);
                            json.BlittableValidation();
                        }
                        ChangeVectorEntry[] conflictingVector;
                        var conflictStatus = GetConflictStatusForDocument(_documentsContext, doc.Id, _tempReplicatedChangeVector, out conflictingVector);

                        switch (conflictStatus)
                        {
                            case ConflictStatus.Update:
                                if (json != null)
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info(
                                            $"Conflict check resolved to Update operation, doing PUT on doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                    _database.DocumentsStorage.Put(_documentsContext, doc.Id, null, json,
                                        _tempReplicatedChangeVector);
                                }
                                else
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info(
                                            $"Conflict check resolved to Update operation, writing tombstone for doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                    _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                                        _documentsContext, doc.Id,
                                        _tempReplicatedChangeVector,
                                        doc.Collection);
                                }
                                break;
                            case ConflictStatus.ShouldResolveConflict:
                                _documentsContext.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_documentsContext, doc.Id);
                                goto case ConflictStatus.Update;
                            case ConflictStatus.Conflict:
                                if (_log.IsInfoEnabled)
                                    _log.Info(
                                        $"Conflict check resolved to Conflict operation, resolving conflict for doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                HandleConflictForDocument(_documentsContext,doc, conflictingVector, json);
                                break;
                            case ConflictStatus.AlreadyMerged:
                                if (_log.IsInfoEnabled)
                                    _log.Info(
                                        $"Conflict check resolved to AlreadyMerged operation, nothing to do for doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                //nothing to do
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(conflictStatus),
                                    "Invalid ConflictStatus: " + conflictStatus);
                        }
                    }
                    _database.DocumentsStorage.SetDatabaseChangeVector(_documentsContext,
                        maxReceivedChangeVectorByDatabase);
                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_documentsContext, ConnectionInfo.SourceDatabaseId,
                        lastEtag);

                    _documentsContext.Transaction.Commit();
                }
                sw.Stop();

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received and written {replicatedDocsCount:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastEtag}.");
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Failed to receive documents replication batch. This is not supposed to happen, and is likely a bug.", e);
                throw;
            }
            finally
            {
                writeBuffer.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ThrowInvalidSize(int totalSize, ReplicationDocumentsPositions doc)
        {
            throw new ArgumentOutOfRangeException(
                $"Reading past the size of buffer! TotalSize {totalSize} but position is {doc.Position} & size is {doc.DocumentSize}!");
        }

        private void EnsureRavenEntityName(BlittableJsonReaderObject obj,string collection)
        {
            DynamicJsonValue mutatedMetadata;
            BlittableJsonReaderObject metadata;
            if (obj.TryGet(Constants.Metadata.Key, out metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }
            else
            {
                obj.Modifications = new DynamicJsonValue(obj)
                {
                    [Constants.Metadata.Key] = mutatedMetadata = new DynamicJsonValue()
                };
            }
            if (mutatedMetadata[Constants.Headers.RavenEntityName] == null)
            {
                mutatedMetadata[Constants.Headers.RavenEntityName] = collection;
            }
        }
        
        public bool TryResovleConflictByScript(ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {
            List<DocumentConflict> conflictedDocs = new List<DocumentConflict>(_documentsContext.DocumentDatabase.DocumentsStorage.GetConflictsFor(_documentsContext, docPosition.Id));
            bool isTomstone = false;
            bool isLocalExists = false;
            long storageId = -1;

            if (conflictedDocs.Count == 0)
            {
                var relevantLocalDoc = _documentsContext.DocumentDatabase.DocumentsStorage
                            .GetDocumentOrTombstone(
                                _documentsContext,
                                docPosition.Id);
                isLocalExists = true;
                if (relevantLocalDoc.Item1 != null)
                {
                    conflictedDocs.Add(relevantLocalDoc.Item1);
                    storageId = relevantLocalDoc.Item1.StorageId;
                }
                else if (relevantLocalDoc.Item2 != null)
                {
                    conflictedDocs.Add(relevantLocalDoc.Item2);
                    storageId = relevantLocalDoc.Item2.StorageId;
                    isTomstone = true;
                }
            }

            if (conflictedDocs.Count == 0)
            {
                throw new InvalidDataException($"Invalid number of conflicted documents {conflictedDocs.Count}");
            }

            conflictedDocs.Add(new DocumentConflict
            {
                LoweredKey = conflictedDocs[0].LoweredKey,
                Key = conflictedDocs[0].Key,
                ChangeVector = _tempReplicatedChangeVector,
                Doc = doc
            });

            var patch = new PatchConflict(_database, conflictedDocs);
            var collection = CollectionName.GetCollectionName(docPosition.Id, doc);

            ScriptResolver scriptResolver;
            var hasScript = _parent.ScriptConflictResolversCache.TryGetValue(collection, out scriptResolver);
            if (!hasScript || scriptResolver == null)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Script not found to resolve the {collection} collection");
                }
                return false;
            }

            var patchRequest = new PatchRequest
            {
                Script = scriptResolver.Script
            };
            BlittableJsonReaderObject resolved;
            if (patch.TryResolveConflict(_documentsContext, patchRequest , out resolved) == false)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Conflict resolution script for {collection} collection declined to resolve the conflict for {docPosition.Id}");
                }
                return false;
            }
            
            _documentsContext.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_documentsContext, docPosition.Id);

            if (isLocalExists)
            {
                _documentsContext.DocumentDatabase.DocumentsStorage.DeleteWithoutCreatingTombstone(_documentsContext,
              collection, storageId, isTomstone);
            }

            var merged = ReplicationUtils.MergeVectors(conflictingVector, _tempReplicatedChangeVector);
            if (resolved != null)
            {
                EnsureRavenEntityName(resolved, collection);
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Conflict resolution script for {collection} collection resolved the conflict for {docPosition.Id}.");
                }

                _database.DocumentsStorage.Put(
                    _documentsContext,
                    docPosition.Id,
                    null,
                    resolved,
                    merged);
            }
            else //resolving to tombstone
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Conflict resolution script for {collection} collection resolved the conflict for {docPosition.Id} by deleting the document, tombstone created");
                }
                _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                    _documentsContext,
                    docPosition.Id,
                    merged,
                    collection);
            }
            return true;
        }

        private void HandleConflictForDocument(
            DocumentsOperationContext context,
            ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {
            if (docPosition.Id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase))
            {
                HandleHiloConflict(context, docPosition, doc);
                return;
            }
            if (_database.DocumentsStorage.TryResolveIdenticalDocument(_documentsContext, docPosition.Id, doc, _tempReplicatedChangeVector) ||
            TryResovleConflictByScript(docPosition, conflictingVector, doc))
            {
                return;
            };

            switch (ReplicationDocument?.DocumentConflictResolution ?? StraightforwardConflictResolution.None)
            {
                case StraightforwardConflictResolution.ResolveToLocal:
                    ResolveConflictToLocal(docPosition, conflictingVector);
                    break;
                case StraightforwardConflictResolution.ResolveToRemote:
                    ResolveConflictToRemote(docPosition, doc, conflictingVector);
                    break;
                case StraightforwardConflictResolution.ResolveToLatest:
                    if (conflictingVector == null) //precaution
                    {
                        throw new InvalidOperationException(
                            "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");
                    }

                    DateTime localLastModified;
                    var relevantLocalConflict = _documentsContext.DocumentDatabase.DocumentsStorage.GetConflictForChangeVector(_documentsContext, docPosition.Id, conflictingVector);
                    if (relevantLocalConflict != null)
                    {
                        localLastModified = relevantLocalConflict.Doc.GetLastModified();
                    }
                    else //the conflict is with existing document/tombstone
                    {
                        var relevantLocalDoc = _documentsContext.DocumentDatabase.DocumentsStorage
                            .GetDocumentOrTombstone(
                                _documentsContext,
                                docPosition.Id);
                        if (relevantLocalDoc.Item1 != null)
                            localLastModified = relevantLocalDoc.Item1.Data.GetLastModified();
                        else if (relevantLocalDoc.Item2 != null)
                        {
                            ResolveConflictToRemote(docPosition, doc, conflictingVector);
                            return;
                        }
                        else //precaution, not supposed to get here
                        {
                            throw new InvalidOperationException(
                                $"Didn't find document neither tombstone for specified id ({docPosition.Id}), this is not supposed to happen and is likely a bug.");
                        }
                    }
                    var remoteLastModified = doc.GetLastModified();
                    if (remoteLastModified > localLastModified)
                    {
                        ResolveConflictToRemote(docPosition, doc, conflictingVector);
                    }
                    else
                    {
                        ResolveConflictToLocal(docPosition, conflictingVector);
                    }
                    break;
                 default:
                    _database.DocumentsStorage.AddConflict(_documentsContext, docPosition.Id, doc, _tempReplicatedChangeVector);
                    break;
            }
        }

        private void HandleHiloConflict(DocumentsOperationContext context, ReplicationDocumentsPositions docPosition,
            BlittableJsonReaderObject doc)
        {
            long highestMax;
            if (!doc.TryGet("Max", out highestMax))
            {
                throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name'Max'");
            }

            var conflicts = _database.DocumentsStorage.GetConflictsFor(context, docPosition.Id);

            var resolvedHiLoDoc = doc;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, docPosition.Id);
                double max;
                if (localHiloDoc.Data.TryGet("Max", out max) && max > highestMax)
                    resolvedHiLoDoc = localHiloDoc.Data;

            }
            else
            {
                foreach (var conflict in conflicts)
                {
                    long tmpMax;
                    if (conflict.Doc.TryGet("Max", out tmpMax) && tmpMax > highestMax)
                    {
                        highestMax = tmpMax;
                        resolvedHiLoDoc = conflict.Doc;
                    }
                }
            }
            _database.DocumentsStorage.Put(context, docPosition.Id, null, resolvedHiLoDoc);
        }

        private void ResolveConflictToRemote(ReplicationDocumentsPositions doc,
            BlittableJsonReaderObject json,
            ChangeVectorEntry[] conflictingVector)
        {
            var merged = ReplicationUtils.MergeVectors(conflictingVector, _tempReplicatedChangeVector);
            _documentsContext.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_documentsContext, doc.Id);
            _database.DocumentsStorage.Put(_documentsContext, doc.Id, null, json, merged);
        }

        private void ResolveConflictToLocal(ReplicationDocumentsPositions doc, ChangeVectorEntry[] conflictingVector)
        {
            var relevantLocalConflict =
                _documentsContext.DocumentDatabase.DocumentsStorage.GetConflictForChangeVector(
                    _documentsContext,
                    doc.Id,
                    conflictingVector);

            var merged = ReplicationUtils.MergeVectors(conflictingVector, _tempReplicatedChangeVector);

            //if we reached the state of conflict, there must be at least one conflicting change vector locally
            //thus, should not happen
            if (relevantLocalConflict != null)
            {
                _documentsContext.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_documentsContext, doc.Id);

                if (relevantLocalConflict.Doc != null)
                {
                    _database.DocumentsStorage.Put(
                        _documentsContext,
                        doc.Id,
                        null,
                        relevantLocalConflict.Doc,
                        merged);
                }
                else //resolving to tombstone
                {
                    _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                        _documentsContext,
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
        private void SendHeartbeatStatusToSource(BlittableJsonTextWriter writer, long lastDocumentEtag, long lastIndexOrTransformerEtag, string handledMessageType)
        {
            var documentChangeVectorAsDynamicJson = new DynamicJsonArray();
            ChangeVectorEntry[] databaseChangeVector;

            using (_documentsContext.OpenReadTransaction())
            {
                databaseChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(_documentsContext);
            }

            foreach (var changeVectorEntry in databaseChangeVector)
            {
                documentChangeVectorAsDynamicJson.Add(new DynamicJsonValue
                {
                    [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                    [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                });
            }

            var indexesChangeVectorAsDynamicJson = new DynamicJsonArray();
            ChangeVectorEntry[] indexesAndTransformersChangeVector;
            using (var tx = _configurationContext.OpenReadTransaction())
                indexesAndTransformersChangeVector =
                    _database.IndexMetadataPersistence.GetIndexesAndTransformersChangeVector(tx.InnerTransaction);

            foreach (var changeVectorEntry in indexesAndTransformersChangeVector)
            {
                indexesChangeVectorAsDynamicJson.Add(new DynamicJsonValue
                {
                    [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                    [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                });
            }

            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"Sending heartbeat ok => {FromToString} with last document etag = {lastDocumentEtag}, last index/transformer etag = {lastIndexOrTransformerEtag} and document change vector: {databaseChangeVector.Format()}");
            }
            _documentsContext.Write(writer, new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = "Ok",
                [nameof(ReplicationMessageReply.MessageType)] = handledMessageType,
                [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastDocumentEtag,
                [nameof(ReplicationMessageReply.LastIndexTransformerEtagAccepted)] = lastIndexOrTransformerEtag,
                [nameof(ReplicationMessageReply.Exception)] = null,
                [nameof(ReplicationMessageReply.DocumentsChangeVector)] = documentChangeVectorAsDynamicJson,
                [nameof(ReplicationMessageReply.IndexTransformerChangeVector)] = indexesChangeVectorAsDynamicJson
            });

            writer.Flush();
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        public ReplicationDocument ReplicationDocument =>
            _replicationDocument ?? (_replicationDocument = _parent.GetReplicationDocument());

        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>();
        private readonly List<ReplicationIndexOrTransformerPositions> _replicatedIndexesAndTransformers = new List<ReplicationIndexOrTransformerPositions>();
        private long _lastDocumentEtag;
        private long _lastIndexOrTransformerEtag;

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
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing IncomingReplicationHandler ({FromToString})");
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
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _disposables.Clear();
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);
        protected void OnIndexesAndTransformersReceived(IncomingReplicationHandler instance) => IndexesAndTransformersReceived?.Invoke(instance);

        private ConflictStatus GetConflictStatusForIndexOrTransformer(TransactionOperationContext context, string name, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = _database.IndexMetadataPersistence.GetConflictsFor(context.Transaction.InnerTransaction, context, name, 0,int.MaxValue).ToList();
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

            var metadata = _database.IndexMetadataPersistence.GetIndexMetadataByName(context.Transaction.InnerTransaction, context, name, false);
            ChangeVectorEntry[] local;

            if (metadata != null)
                local = metadata.ChangeVector;
            else
                return ConflictStatus.Update; //index/transformer with 'name' doesn't exist locally, so just do PUT


            var status = GetConflictStatus(remote, local);
            if (status == ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }

            return status;
        }

        private ConflictStatus GetConflictStatusForDocument(DocumentsOperationContext context, string key, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
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
            if(local == null)
                return ConflictStatus.Update;

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

            if(remoteHasLargerEntries == false && localHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical

            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;
        }
    }
}
