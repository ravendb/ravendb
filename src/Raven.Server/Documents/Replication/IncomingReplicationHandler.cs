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
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json.Parsing;
using System.Linq;
using System.Net;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Utils;
using Voron;
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

        public ReplicationStatistics.IncomingBatchStats IncomingStats = new ReplicationStatistics.IncomingBatchStats();

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

            var replicationDoc = _parent.ReplicationDocument;
            var scripts = _parent.ScriptConflictResolversCache;
            _conflictManager = new ConflictManager(_database, replicationDoc, scripts);
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
            IncomingStats = new ReplicationStatistics.IncomingBatchStats
            {
                Status = ReplicationStatus.Received,
                RecievedTime = DateTime.UtcNow,
                Source = FromToString,
                RecievedEtag = lastDocumentEtag,
                DocumentsCount = itemCount
            };
            ReceiveSingleDocumentsBatch(documentsContext, itemCount, lastDocumentEtag);
            IncomingStats.DoneReplicateTime = DateTime.UtcNow;
            _parent.RepliactionStats.Add(IncomingStats);
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
                    ReplicationUtils.ConflictStatus conflictStatus;
                    using (configurationContext.OpenReadTransaction())
                        conflictStatus = GetConflictStatusForIndexOrTransformer(configurationContext, item.Name,
                            remote,
                            out conflictingVector);

                    ReadChangeVector(item, maxReceivedChangeVectorByDatabase);

                    using (var definition = new BlittableJsonReaderObject(buffer + item.Position, item.DefinitionSize,configurationContext))
                    {
                        switch (conflictStatus)
                        {
                            //note : PutIndexOrTransformer() is deleting conflicts and merges chnage vectors
                            //of the conflicts. This can be seen in IndexesEtagsStorage::WriteEntry()
                            case ReplicationUtils.ConflictStatus.Update:                                
                                PutIndexOrTransformer(configurationContext, item, definition);
                                break;
                            case ReplicationUtils.ConflictStatus.Conflict:
                                using (var txw = configurationContext.OpenWriteTransaction())
                                {
                                    HandleConflictForIndexOrTransformer(item, definition, conflictingVector, txw, configurationContext);

                                    UpdateIndexesChangeVector(txw, lastEtag, maxReceivedChangeVectorByDatabase);
                                    LastIndexOrTransformerEtag = lastEtag;
                                    txw.Commit();
                                    return; // skip the UpdateIndexesChangeVector below to avoid duplicate calls
                                }
                            case ReplicationUtils.ConflictStatus.AlreadyMerged:
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

                var replicationCommand = new MergedDocumentReplicationCommand(this, buffer, totalSize, lastEtag);
                _database.TxMerger.Enqueue(replicationCommand).Wait();

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
        private readonly ConflictManager _conflictManager;

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public short TransactionMarker;
            public int DocumentSize;
            public string Collection;
            public long LastModifiedTicks;
            public DocumentFlags Flags;
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

                curDoc.Flags = *(DocumentFlags*)ReadExactly(sizeof(DocumentFlags));

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

            if (_incomingThread != Thread.CurrentThread &&
                _incomingThread?.ThreadState != ThreadState.Unstarted)
            {
                _incomingThread?.Join();
            }

            _incomingThread = null;
            _cts.Dispose();
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);
        protected void OnIndexesAndTransformersReceived(IncomingReplicationHandler instance) => IndexesAndTransformersReceived?.Invoke(instance);

        private ReplicationUtils.ConflictStatus GetConflictStatusForIndexOrTransformer(TransactionOperationContext context, string name, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = _database.IndexMetadataPersistence.GetConflictsFor(context.Transaction.InnerTransaction, context, name, 0,int.MaxValue).ToList();
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    if (ReplicationUtils.GetConflictStatus(remote, existingConflict.ChangeVector) == ReplicationUtils.ConflictStatus.Conflict)
                    {
                        conflictingVector = existingConflict.ChangeVector;
                        return ReplicationUtils.ConflictStatus.Conflict;
                    }
                }

                return ReplicationUtils.ConflictStatus.Update;
            }

            var metadata = _database.IndexMetadataPersistence.GetIndexMetadataByName(context.Transaction.InnerTransaction, context, name, false);
            ChangeVectorEntry[] local;

            if (metadata != null)
                local = metadata.ChangeVector;
            else
                return ReplicationUtils.ConflictStatus.Update; //index/transformer with 'name' doesn't exist locally, so just do PUT


            var status = ReplicationUtils.GetConflictStatus(remote, local);
            if (status == ReplicationUtils.ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }

            return status;
        }
        
        public unsafe class MergedDocumentReplicationCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly IncomingReplicationHandler _incoming;

            private ChangeVectorEntry[] _changeVector = new ChangeVectorEntry[0];

            private long _lastEtag;
            private byte* _buffer;
            private int _totalSize;
          
            public MergedDocumentReplicationCommand(IncomingReplicationHandler incoming, byte* buffer, int totalSize, long lastEtag)
            {
                _incoming = incoming;
                _buffer = buffer;
                _totalSize = totalSize;
                _lastEtag = lastEtag;
            }

            public override void Execute(DocumentsOperationContext context)
            {
                var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                var database = _incoming._database;
                foreach (var changeVectorEntry in database.DocumentsStorage.GetDatabaseChangeVector(context))
                {
                    maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                }

                foreach (var docPosition in _incoming._replicatedDocs)
                {
                    context.TransactionMarkerOffset = docPosition.TransactionMarker;
                    BlittableJsonReaderObject document = null;
                    try
                    {
                        ReadChangeVector(docPosition, _buffer, maxReceivedChangeVectorByDatabase);
                        if (docPosition.DocumentSize >= 0) //no need to load document data for tombstones
                            // document size == -1 --> doc is a tombstone
                        {
                            if (docPosition.Position + docPosition.DocumentSize > _totalSize)
                                ThrowInvalidSize(_totalSize, docPosition);

                            //if something throws at this point, this means something is really wrong and we should stop receiving documents.
                            //the other side will receive negative ack and will retry sending again.
                            document = new BlittableJsonReaderObject(
                                _buffer + docPosition.Position + (docPosition.ChangeVectorCount*sizeof(ChangeVectorEntry)),
                                docPosition.DocumentSize, context);
                            document.BlittableValidation();
                        }

                        if ((docPosition.Flags & DocumentFlags.FromVersionStorage) == DocumentFlags.FromVersionStorage)
                        {
                            if (database.BundleLoader.VersioningStorage == null)
                            {
                                if (_incoming._log.IsOperationsEnabled)
                                    _incoming._log.Operations("Versioing storage is disabled but the node got a versioned document from replication.");
                                continue;
                            }
                            database.BundleLoader.VersioningStorage.PutFromDocument(context, docPosition.Id, document, _changeVector);
                            continue;
                        }

                        ChangeVectorEntry[] conflictingVector;
                        var conflictStatus = ReplicationUtils.GetConflictStatusForDocument(context, docPosition.Id, _changeVector, out conflictingVector);

                        switch (conflictStatus)
                        {
                            case ReplicationUtils.ConflictStatus.Update:
                                if (document != null)
                                {
                                    if (_incoming._log.IsInfoEnabled)
                                        _incoming._log.Info(
                                            $"Conflict check resolved to Update operation, doing PUT on doc = {docPosition.Id}, with change vector = {_changeVector.Format()}");
                                    database.DocumentsStorage.Put(context, docPosition.Id, null, document,
                                        docPosition.LastModifiedTicks,
                                        _changeVector, DocumentFlags.FromReplication);
                                }
                                else
                                {
                                    if (_incoming._log.IsInfoEnabled)
                                        _incoming._log.Info(
                                            $"Conflict check resolved to Update operation, writing tombstone for doc = {docPosition.Id}, with change vector = {_changeVector.Format()}");
                                    Slice keySlice;
                                    using (DocumentKeyWorker.GetSliceFromKey(context, docPosition.Id, out keySlice))
                                    {
                                        database.DocumentsStorage.Delete(
                                            context, keySlice, docPosition.Id, null,
                                            docPosition.LastModifiedTicks,
                                            _changeVector,
                                            context.GetLazyString(docPosition.Collection));
                                    }
                                }
                                break;
                            case ReplicationUtils.ConflictStatus.Conflict:
                                if (_incoming._log.IsInfoEnabled)
                                    _incoming._log.Info(
                                        $"Conflict check resolved to Conflict operation, resolving conflict for doc = {docPosition.Id}, with change vector = {_changeVector.Format()}");
                                _incoming._conflictManager.HandleConflictForDocument(context, docPosition, document, _changeVector, conflictingVector);
                                break;
                            case ReplicationUtils.ConflictStatus.AlreadyMerged:
                                if (_incoming._log.IsInfoEnabled)
                                    _incoming._log.Info(
                                        $"Conflict check resolved to AlreadyMerged operation, nothing to do for doc = {docPosition.Id}, with change vector = {_changeVector.Format()}");
                                //nothing to do
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(conflictStatus),
                                    "Invalid ConflictStatus: " + conflictStatus);
                        }
                    }
                    finally
                    {
                        document?.Dispose();
                    }
                }
                database.DocumentsStorage.SetDatabaseChangeVector(context,
                    maxReceivedChangeVectorByDatabase);
                database.DocumentsStorage.SetLastReplicateEtagFrom(context, _incoming.ConnectionInfo.SourceDatabaseId,
                    _lastEtag);
            }

            private void ReadChangeVector(ReplicationDocumentsPositions doc, 
                byte* buffer, Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
            {
                if (_changeVector.Length != doc.ChangeVectorCount)
                {
                    _changeVector = new ChangeVectorEntry[doc.ChangeVectorCount];
                }

                for (int i = 0; i < doc.ChangeVectorCount; i++)
                {
                    _changeVector[i] = ((ChangeVectorEntry*)(buffer + doc.Position))[i];

                    long etag;
                    if (maxReceivedChangeVectorByDatabase.TryGetValue(_changeVector[i].DbId, out etag) == false ||
                        etag > _changeVector[i].Etag)
                    {
                        maxReceivedChangeVectorByDatabase[_changeVector[i].DbId] = _changeVector[i].Etag;
                    }
                }
            }
        }
    }
}
