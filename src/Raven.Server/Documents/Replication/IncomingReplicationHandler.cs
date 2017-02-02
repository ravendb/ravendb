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
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using System.Linq;
using System.Net;
using Raven.Server.Documents.Patch;
using Constants = Raven.Abstractions.Data.Constants;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using ThreadState = System.Threading.ThreadState;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private readonly Stream _stream;
        private readonly DocumentReplicationLoader _parent;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;
        public event Action<IncomingReplicationHandler> IndexesAndTransformersReceived;

        public long LastDocumentEtag;
        public long LastIndexOrTransformerEtag;

        public long LastHeartbeatTicks;

        public IncomingReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest replicatedLastEtag,
            DocumentReplicationLoader parent)
        {
            _connectionOptions = options;
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            
            _database = options.DocumentDatabase;
            _tcpClient = options.TcpClient;
            _stream = options.Stream;
            ConnectionInfo.RemoteIp = ((IPEndPoint)_tcpClient.Client.RemoteEndPoint).Address.ToString();
            _parent = parent;

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
                
                using (_connectionOptions.ConnectionProcessingInProgress())
                using (_stream)
                using (var interruptibleRead = new InterruptibleRead(
                            _database.DocumentsStorage.ContextPool,
                            _stream))
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        try
                        {
                            using (var msg = interruptibleRead.ParseToMemory(
                                _replicationFromAnotherSource,
                                "IncomingReplication/read-message",
                                Timeout.Infinite,
                                _connectionOptions.PinnedBuffer,
                                _database.DatabaseShutdown))
                            {
                                TransactionOperationContext configurationContext;
                                if (msg.Document != null)
                                {
                                    using (var writer = new BlittableJsonTextWriter(msg.Context, _stream))
                                    using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(
                                            out configurationContext))
                                    {
                                        HandleSingleReplicationBatch(msg.Context, configurationContext,
                                            msg.Document,
                                            writer);
                                    }
                                }
                                else // notify peer about new change vector
                                {
                                    DocumentsOperationContext documentsContext;

                                    using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(
                                        out configurationContext))
                                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(
                                            out documentsContext))
                                    using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
                                    {
                                        SendHeartbeatStatusToSource(
                                            documentsContext,
                                            configurationContext,
                                            writer,
                                            _lastDocumentEtag,
                                            _lastIndexOrTransformerEtag,
                                            "Notify");
                                    }
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
                                if (e.InnerException is SocketException)
                                    _log.Info(
                                        "Failed to read data from incoming connection. The incoming connection will be closed and re-created.",
                                        e);
                                else
                                    _log.Info(
                                        "Received unexpected exception while receiving replication batch. This is not supposed to happen.",
                                        e);
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

        private void HandleSingleReplicationBatch(
            DocumentsOperationContext documentsContext, 
            TransactionOperationContext configurationContext, 
            BlittableJsonReaderObject message, 
            BlittableJsonTextWriter writer)
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
                        HandleReceivedDocumentBatch(documentsContext, message, _lastDocumentEtag);
                        break;
                    case ReplicationMessageType.IndexesTransformers:
                        HandleReceivedIndexOrTransformerBatch(configurationContext, message, _lastIndexOrTransformerEtag);
                        break;
                    case ReplicationMessageType.Heartbeat:
                        //nothing to do..
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                SendHeartbeatStatusToSource(documentsContext, configurationContext, writer, _lastDocumentEtag, _lastIndexOrTransformerEtag, messageType);
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

                    documentsContext.Write(writer, returnValue);
                    writer.Flush();

                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed replicating documents from {FromToString}.", e);

                    throw;
                }
            }
        }

        private void HandleReceivedIndexOrTransformerBatch(TransactionOperationContext configurationContext, BlittableJsonReaderObject message, long lastIndexOrTransformerEtag)
        {
            int itemCount;
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemCount), out itemCount))
                throw new InvalidDataException("Expected the 'ItemCount' field, but had no numeric field of this value, this is likely a bug");
            var replicatedIndexTransformerCount = itemCount;

            if (replicatedIndexTransformerCount <= 0)
                return;

            ReceiveSingleIndexAndTransformersBatch(configurationContext, replicatedIndexTransformerCount, lastIndexOrTransformerEtag);
            OnIndexesAndTransformersReceived(this);
        }

        private void HandleReceivedDocumentBatch(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message, long lastDocumentEtag)
        {
            int itemCount;
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemCount), out itemCount))
                throw new InvalidDataException(
                    "Expected the 'ItemCount' field, but had no numeric field of this value, this is likely a bug");

            string resovlerId;
            int? resolverVersion;
            if (message.TryGet(nameof(ReplicationMessageHeader.ResolverId), out resovlerId) &&
                message.TryGet(nameof(ReplicationMessageHeader.ResolverVersion), out resolverVersion))
            {
                _parent.UpdateReplicationDocumentWithResolver(resovlerId, resolverVersion);
            }

            ReceiveSingleDocumentsBatch(documentsContext, itemCount, lastDocumentEtag);

            OnDocumentsReceived(this);
        }

        private unsafe void ReceiveSingleIndexAndTransformersBatch(
            TransactionOperationContext configurationContext,
            int itemCount, 
            long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            var writeBuffer = configurationContext.GetStream();
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
                using (var tx = configurationContext.OpenReadTransaction())
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
                    using (configurationContext.OpenReadTransaction())
                        conflictStatus = GetConflictStatusForIndexOrTransformer(configurationContext, item.Name,
                            remote,
                            out conflictingVector);

                    ReadChangeVector(item, maxReceivedChangeVectorByDatabase);

                    using (var definition = new BlittableJsonReaderObject(buffer + item.Position, item.DefinitionSize,configurationContext))
                    {
                        switch (conflictStatus)
                        {
                            case ConflictStatus.ShouldResolveConflict:
                            //note : PutIndexOrTransformer() is deleting conflicts and merges chnage vectors
                            //of the conflicts. This can be seen in IndexesEtagsStorage::WriteEntry()
                            case ConflictStatus.Update:                                
                                PutIndexOrTransformer(configurationContext, item, definition);
                                break;
                            case ConflictStatus.Conflict:
                                using (var txw = configurationContext.OpenWriteTransaction())
                                {
                                    HandleConflictForIndexOrTransformer(item, definition, conflictingVector, txw, configurationContext);

                                    UpdateIndexesChangeVector(txw, lastEtag, maxReceivedChangeVectorByDatabase);
                                    LastIndexOrTransformerEtag = lastEtag;
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

                        using (var txw = configurationContext.OpenWriteTransaction())
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

        private void UpdateIndexesChangeVector(
            RavenTransaction txw,
            long lastEtag,
            Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            _database.IndexMetadataPersistence.SetGlobalChangeVector(txw.InnerTransaction,
                txw.InnerTransaction.Allocator, maxReceivedChangeVectorByDatabase);
            _database.IndexMetadataPersistence.SetLastReplicateEtagFrom(txw.InnerTransaction,
                txw.InnerTransaction.Allocator, ConnectionInfo.SourceDatabaseId, lastEtag);
        }

        private void HandleConflictForIndexOrTransformer(
            ReplicationIndexOrTransformerPositions item, 
            BlittableJsonReaderObject definition, 
            ChangeVectorEntry[] conflictingVector,
            RavenTransaction tx, 
            TransactionOperationContext configurationContext)
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

                    _database.IndexMetadataPersistence.AddConflict(configurationContext, configurationContext.Transaction.InnerTransaction,
                        item.Name, item.Type, conflictingVector, definition);

                    // this is severe enough to warrant an alert
                    _database.NotificationCenter.AddAfterTransactionCommit(
                        AlertRaised.Create("Replication conflict", msg, AlertType.Replication,
                            NotificationSeverity.Warning, key: replicationSource), tx);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PutIndexOrTransformer(
            TransactionOperationContext configurationContext,
            ReplicationIndexOrTransformerPositions item, 
            BlittableJsonReaderObject definition)
        {
            switch (item.Type)
            {
                case IndexEntryType.Index:
                    PutIndexReplicationItem(configurationContext, item, definition);
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

        private void PutIndexReplicationItem(
            TransactionOperationContext configurationContext,
            ReplicationIndexOrTransformerPositions item,
            BlittableJsonReaderObject definition)
        {
            var existing = _database.IndexStore.GetIndex(item.Name);

            if (existing != null)
            {
                using (var existingDefinition = configurationContext.ReadObject(existing.GetIndexDefinition().ToJson(),
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
            for (int x = 0; x < itemCount; x++)
            {
                var curItem = new ReplicationIndexOrTransformerPositions
                {
                    Position = writeBuffer.SizeInBytes
                };

                var changeVectorCount = *(int*)ReadExactly(sizeof(int));

                var changeVectorSize = sizeof(ChangeVectorEntry)*changeVectorCount;
                ;
                curItem.ChangeVector = new ChangeVectorEntry[changeVectorCount];
                fixed (ChangeVectorEntry* pChangeVector = curItem.ChangeVector)
                    Memory.Copy((byte*) pChangeVector, ReadExactly(changeVectorSize), changeVectorSize);

                curItem.Etag = *(long*)ReadExactly(sizeof(long));

                int typeAsInt = *(int*)ReadExactly(sizeof(int));
                curItem.Type = (IndexEntryType) typeAsInt;

                var nameSize = *(int*)ReadExactly(sizeof(int));

                var charCount = *(int*)ReadExactly(sizeof(int));

                curItem.Name = new string(' ', charCount);
                fixed (char* pName = curItem.Name)
                    Encoding.UTF8.GetChars(ReadExactly(nameSize), nameSize, pName, charCount);

                var definitionSize = curItem.DefinitionSize = *(int*)ReadExactly(sizeof(int));
                ReadExactly(definitionSize, ref writeBuffer);

                _replicatedIndexesAndTransformers.Add(curItem);
            }
        }

        private unsafe void ReadExactly(int size, ref UnmanagedWriteBuffer into)
        {
            while(size > 0)
            {
                var available = _connectionOptions.PinnedBuffer.Valid - _connectionOptions.PinnedBuffer.Used;
                if (available == 0)
                {
                    var read = _connectionOptions.Stream.Read(_connectionOptions.PinnedBuffer.Buffer.Array,
                      _connectionOptions.PinnedBuffer.Buffer.Offset,
                      _connectionOptions.PinnedBuffer.Buffer.Count);
                    if (read == 0)
                        throw new EndOfStreamException();

                    _connectionOptions.PinnedBuffer.Valid = read;
                    _connectionOptions.PinnedBuffer.Used = 0;
                    continue;
                }
                var min = Math.Min(size, available);
                var result = _connectionOptions.PinnedBuffer.Pointer + _connectionOptions.PinnedBuffer.Used;
                into.Write(result, min);
                _connectionOptions.PinnedBuffer.Used += min;
                size -= min;
            }
        }

        private unsafe byte* ReadExactly(int size)
        {
            var diff = _connectionOptions.PinnedBuffer.Valid - _connectionOptions.PinnedBuffer.Used;
            if (diff >= size)
            {
                var result = _connectionOptions.PinnedBuffer.Pointer + _connectionOptions.PinnedBuffer.Used;
                _connectionOptions.PinnedBuffer.Used += size;
                return result;
            }
            return ReadExactlyUnlikely(size, diff);
        }

        private unsafe byte* ReadExactlyUnlikely(int size, int diff)
        {
            for (int i = diff - 1; i >= 0; i--)
            {
                _connectionOptions.PinnedBuffer.Pointer[i] =
                    _connectionOptions.PinnedBuffer.Pointer[_connectionOptions.PinnedBuffer.Used + i];
            }
            _connectionOptions.PinnedBuffer.Valid = diff;
            _connectionOptions.PinnedBuffer.Used = 0;
            while (diff < size)
            {
                var read = _connectionOptions.Stream.Read(_connectionOptions.PinnedBuffer.Buffer.Array,
                    _connectionOptions.PinnedBuffer.Buffer.Offset + diff,
                    _connectionOptions.PinnedBuffer.Buffer.Count - diff);
                if (read == 0)
                    throw new EndOfStreamException();

                _connectionOptions.PinnedBuffer.Valid += read;
                diff += read;
            }
            var result = _connectionOptions.PinnedBuffer.Pointer + _connectionOptions.PinnedBuffer.Used;
            _connectionOptions.PinnedBuffer.Used += size;
            return result;
        }
        
        private unsafe void ReceiveSingleDocumentsBatch(DocumentsOperationContext documentsContext, int replicatedDocsCount, long lastEtag)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Receiving replication batch with {replicatedDocsCount} documents starting with {lastEtag} from {ConnectionInfo}");
            }

            var sw = Stopwatch.StartNew();
            var writeBuffer = documentsContext.GetStream();
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

                using (documentsContext.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(documentsContext))
                    {
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }

                    foreach (var doc in _replicatedDocs)
                    {
                        documentsContext.TransactionMarkerOffset = doc.TransactionMarker;

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
                                doc.DocumentSize, documentsContext);
                            json.BlittableValidation();
                        }
                        
                        ChangeVectorEntry[] conflictingVector;
                        var conflictStatus = GetConflictStatusForDocument(documentsContext, doc.Id, _tempReplicatedChangeVector, out conflictingVector);

                        switch (conflictStatus)
                        {
                            case ConflictStatus.Update:
                                if (json != null)
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info(
                                            $"Conflict check resolved to Update operation, doing PUT on doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                    _database.DocumentsStorage.Put(documentsContext, doc.Id, null, json,
                                        doc.LastModifiedTicks,
                                        _tempReplicatedChangeVector);
                                }
                                else
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info(
                                            $"Conflict check resolved to Update operation, writing tombstone for doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                    _database.DocumentsStorage.AddTombstoneOnReplicationIfRelevant(
                                        documentsContext, doc.Id,
                                        doc.LastModifiedTicks,
                                        _tempReplicatedChangeVector,
                                        doc.Collection);
                                }
                                break;
                            case ConflictStatus.ShouldResolveConflict:
                                documentsContext.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(documentsContext, doc.Id);
                                goto case ConflictStatus.Update;
                            case ConflictStatus.Conflict:
                                if (_log.IsInfoEnabled)
                                    _log.Info(
                                        $"Conflict check resolved to Conflict operation, resolving conflict for doc = {doc.Id}, with change vector = {_tempReplicatedChangeVector.Format()}");
                                HandleConflictForDocument(documentsContext,doc, conflictingVector, json);
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
                    _database.DocumentsStorage.SetDatabaseChangeVector(documentsContext,
                        maxReceivedChangeVectorByDatabase);
                    _database.DocumentsStorage.SetLastReplicateEtagFrom(documentsContext, ConnectionInfo.SourceDatabaseId,
                        lastEtag);
                    LastDocumentEtag = lastEtag;
                    documentsContext.Transaction.Commit();

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

        

        public bool TryResovleConflictByScript(
            DocumentsOperationContext documentsContext,
            ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {
            List<DocumentConflict> conflictedDocs = new List<DocumentConflict>(documentsContext.DocumentDatabase.DocumentsStorage.GetConflictsFor(documentsContext, docPosition.Id));
            bool isTomstone = false;

            if (conflictedDocs.Count == 0)
            {
                var relevantLocalDoc = documentsContext.DocumentDatabase.DocumentsStorage
                            .GetDocumentOrTombstone(
                                documentsContext,
                                docPosition.Id);
                if (relevantLocalDoc.Item1 != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(documentsContext, relevantLocalDoc.Item1));
                }
                else if (relevantLocalDoc.Item2 != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(relevantLocalDoc.Item2));
                    isTomstone = true;
                }
            }

            if (conflictedDocs.Count == 0)
            {
                InvalidConflictWhenThereIsNone(docPosition);
            }

            var collection = CollectionName.GetCollectionName(docPosition.Id, doc);

            conflictedDocs.Add(new DocumentConflict
            {
                LoweredKey = conflictedDocs[0].LoweredKey,
                Key = conflictedDocs[0].Key,
                Collection = documentsContext.GetLazyStringForFieldWithCaching(collection),
                ChangeVector = _tempReplicatedChangeVector,
                Doc = doc
            });

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

            return _database.DocumentsStorage.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs, 
                documentsContext.GetLazyString(collection), 
                hasLocalTombstone: isTomstone);
        }

        private static void InvalidConflictWhenThereIsNone(ReplicationDocumentsPositions docPosition)
        {
            throw new InvalidDataException(
                $"Conflict detected on {docPosition.Id} but there are no conflicts / docs / tombstones for this document");
        }

        private bool TryResolveUsingDefaultResolver(
            DocumentsOperationContext context, 
            ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {

            var conflicts = new List<DocumentConflict>(_database.DocumentsStorage.GetConflictsFor(context, docPosition.Id));
            var localDocumentTuple = _database.DocumentsStorage.GetDocumentOrTombstone(context, docPosition.Id, throwOnConflict: false);
            var localDoc = DocumentConflict.From(context, localDocumentTuple.Item1) ??
                           DocumentConflict.From(localDocumentTuple.Item2);
            if (localDoc != null)
            {
                conflicts.Add(localDoc);
            }
            conflicts.Add(new DocumentConflict
            {
                ChangeVector = conflictingVector,
                Collection = context.GetLazyStringForFieldWithCaching(
                    docPosition.Collection ?? 
                    CollectionName.GetCollectionName(docPosition.Id, doc)),
                Doc = doc,
                LoweredKey = context.GetLazyString(docPosition.Id)
            });

            return _database.DocumentsStorage.TryResolveUsingDefaultResolverInternal(
                context , 
                _parent.ReplicationDocument?.DefaultResolver, 
                conflicts, 
                localDocumentTuple.Item2 != null);
        }
        
        private void HandleConflictForDocument(
            DocumentsOperationContext documentsContext,
            ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] conflictingVector,
            BlittableJsonReaderObject doc)
        {
            if (docPosition.Id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase))
            {
                HandleHiloConflict(documentsContext, docPosition, doc);
                return;
            }
            if (_database.DocumentsStorage.TryResolveIdenticalDocument(
                documentsContext, 
                docPosition.Id, 
                doc,
                docPosition.LastModifiedTicks,
                _tempReplicatedChangeVector))
                return;

            if (TryResovleConflictByScript(
                documentsContext, 
                docPosition, 
                conflictingVector, 
                doc))
                return;

            if(TryResolveUsingDefaultResolver(
                documentsContext,
                docPosition,
                _tempReplicatedChangeVector,
                doc))
                return;

            switch (_parent.ReplicationDocument?.DocumentConflictResolution ?? StraightforwardConflictResolution.None)
            {
                case StraightforwardConflictResolution.ResolveToLatest:
                    if (conflictingVector == null) //precaution
                    {
                        throw new InvalidOperationException(
                            "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");
                    }

                    var conflicts = new List<DocumentConflict>
                    {
                        new DocumentConflict
                        {
                            Doc = doc,
                            Collection = documentsContext.GetLazyStringForFieldWithCaching(
                                docPosition.Collection ??
                                CollectionName.GetCollectionName(doc)
                                ),
                            LastModified = new DateTime(docPosition.LastModifiedTicks),
                            LoweredKey = documentsContext.GetLazyString(docPosition.Id),
                            ChangeVector = _tempReplicatedChangeVector
                        }
                    };
                    conflicts.AddRange(documentsContext.DocumentDatabase.DocumentsStorage.GetConflictsFor(
                        documentsContext, docPosition.Id));
                    var localDocumentTuple =
                        documentsContext.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(documentsContext,
                            docPosition.Id, throwOnConflict: false);
                    var local = DocumentConflict.From(documentsContext, localDocumentTuple.Item1) ?? DocumentConflict.From(localDocumentTuple.Item2);
                    if (local != null)
                    {
                        conflicts.Add(local);
                    }

                    _database.DocumentsStorage.ResolveToLatest(documentsContext, conflicts, local != null && local.Doc == null);
                    break;
                 default:
                    _database.DocumentsStorage.AddConflict(documentsContext, docPosition, doc, _tempReplicatedChangeVector, docPosition.Collection);
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
        private void SendHeartbeatStatusToSource(DocumentsOperationContext documentsContext, TransactionOperationContext configurationContext, BlittableJsonTextWriter writer, long lastDocumentEtag, long lastIndexOrTransformerEtag, string handledMessageType)
        {            
            var documentChangeVectorAsDynamicJson = new DynamicJsonArray();
            ChangeVectorEntry[] databaseChangeVector;

            using (documentsContext.OpenReadTransaction())
            {
                databaseChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(documentsContext);
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
            using (var tx = configurationContext.OpenReadTransaction())
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
            var defaultResolver = _parent.ReplicationDocument?.DefaultResolver;
            var heartbeat = new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = "Ok",
                [nameof(ReplicationMessageReply.MessageType)] = handledMessageType,
                [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastDocumentEtag,
                [nameof(ReplicationMessageReply.LastIndexTransformerEtagAccepted)] = lastIndexOrTransformerEtag,
                [nameof(ReplicationMessageReply.Exception)] = null,
                [nameof(ReplicationMessageReply.DocumentsChangeVector)] = documentChangeVectorAsDynamicJson,
                [nameof(ReplicationMessageReply.IndexTransformerChangeVector)] = indexesChangeVectorAsDynamicJson,
                [nameof(ReplicationMessageReply.DatabaseId)] = _database.DbId.ToString(),
                [nameof(ReplicationMessageReply.ResolverId)] = defaultResolver?.ResolvingDatabaseId,
                [nameof(ReplicationMessageReply.ResolverVersion)] = defaultResolver?.Version
            };
           
            documentsContext.Write(writer, heartbeat);

            writer.Flush();
            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>();
        private readonly List<ReplicationIndexOrTransformerPositions> _replicatedIndexesAndTransformers = new List<ReplicationIndexOrTransformerPositions>();
        private long _lastDocumentEtag;
        private long _lastIndexOrTransformerEtag;
        private readonly TcpConnectionOptions _connectionOptions;

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public short TransactionMarker;
            public int DocumentSize;
            public string Collection;
            public long LastModifiedTicks;
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
            for (int x = 0; x < replicatedDocs; x++)
            {
                var curDoc = new ReplicationDocumentsPositions
                {
                    Position = writeBuffer.SizeInBytes
                };

                curDoc.ChangeVectorCount = *(int*) ReadExactly(sizeof(int));

                
                writeBuffer.Write(ReadExactly(sizeof(ChangeVectorEntry) * curDoc.ChangeVectorCount), sizeof(ChangeVectorEntry)*curDoc.ChangeVectorCount);

                curDoc.TransactionMarker = *(short*)ReadExactly(sizeof(short));

                curDoc.LastModifiedTicks = *(long*)ReadExactly(sizeof(long));

                var keySize = *(int*)ReadExactly(sizeof(int));
                
                curDoc.Id = Encoding.UTF8.GetString(ReadExactly(keySize), keySize);

                var documentSize = curDoc.DocumentSize = *(int*)ReadExactly(sizeof(int));
                if (documentSize != -1) //if -1, then this is a tombstone
                {
                    ReadExactly(documentSize,ref writeBuffer);
                }
                else
                {
                    //read the collection
                    var collectionSize = *(int*)ReadExactly(sizeof(int));
                    if (collectionSize != -1)
                    {
                        curDoc.Collection = Encoding.UTF8.GetString(ReadExactly(collectionSize), collectionSize);
                    }
                }

                _replicatedDocs.Add(curDoc);
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

            try
            {
                _connectionOptions.Dispose();
            }
            catch
            {
                // do nothing
            }

            _replicationFromAnotherSource.Set();

            if (_incomingThread != Thread.CurrentThread)
            {
                _incomingThread?.Join();
            }

            _incomingThread = null;
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
