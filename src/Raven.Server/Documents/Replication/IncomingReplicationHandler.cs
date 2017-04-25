using System;
using System.Collections.Concurrent;
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
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Utils;
using Voron;
using Memory = Sparrow.Memory;
using ThreadState = System.Threading.ThreadState;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private readonly Stream _stream;
        private readonly ReplicationLoader _parent;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;
        public event Action<IncomingReplicationHandler> IndexesAndTransformersReceived;

        public long LastDocumentEtag;
        public long LastIndexOrTransformerEtag;

        public long LastHeartbeatTicks;

        private readonly ConcurrentQueue<IncomingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<IncomingReplicationStatsAggregator>();

        private IncomingReplicationStatsAggregator _lastStats;

        public IncomingReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest replicatedLastEtag,
            ReplicationLoader parent)
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
            
            _conflictManager = new ConflictManager(_database, _parent.ConflictResolver);
        }

        public IncomingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            var lastStats = _lastStats;

            return _lastReplicationStats
                .Select(x => x == lastStats ? x.ToReplicationPerformanceLiveStatsWithDetails() : x.ToReplicationPerformanceStats())
                .ToArray();
        }


        public IncomingReplicationStatsAggregator GetLatestReplicationPerformance()
        {
            return _lastStats;
        }

        public void Start()
        {
            if (_incomingThread != null)
                return;

            var result = Interlocked.CompareExchange(ref _incomingThread, new Thread(ReceiveReplicationBatches)
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

        private void ReceiveReplicationBatches()
        {
            IsIncomingReplicationThread = true;
            try
            {
                using (_connectionOptions.ConnectionProcessingInProgress("Replication"))
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

                                    using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(
                                        out configurationContext))
                                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(
                                            out DocumentsOperationContext documentsContext))
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
                        var stats = _lastStats = new IncomingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                        AddReplicationPerformance(stats);

                        try
                        {
                            using (var scope = stats.CreateScope())
                            {
                                try
                                {
                                    scope.RecordLastEtag(_lastDocumentEtag);

                                    HandleReceivedDocumentsAndAttachmentsBatch(documentsContext, message, _lastDocumentEtag, scope);
                                    break;

                                }
                                catch (Exception e)
                                {
                                    scope.AddError(e);
                                    throw;
                                }
                            }
                        }
                        finally
                        {
                            stats.Complete();
                        }
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
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemsCount), out int itemsCount))
                throw new InvalidDataException("Expected the 'ItemsCount' field, but had no numeric field of this value, this is likely a bug");
            var replicatedIndexTransformerCount = itemsCount;

            if (replicatedIndexTransformerCount <= 0)
                return;

            ReceiveSingleIndexAndTransformersBatch(configurationContext, replicatedIndexTransformerCount, lastIndexOrTransformerEtag);
            OnIndexesAndTransformersReceived(this);
        }

        private void HandleReceivedDocumentsAndAttachmentsBatch(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message, long lastDocumentEtag, IncomingReplicationStatsScope stats)
        {
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemsCount), out int itemsCount))
                throw new InvalidDataException(
                    $"Expected the '{nameof(ReplicationMessageHeader.ItemsCount)}' field, but had no numeric field of this value, this is likely a bug");

            if (!message.TryGet(nameof(ReplicationMessageHeader.AttachmentStreamsCount), out int attachmentStreamCount))
                throw new InvalidDataException(
                    $"Expected the '{nameof(ReplicationMessageHeader.AttachmentStreamsCount)}' field, but had no numeric field of this value, this is likely a bug");


            ReceiveSingleDocumentsBatch(documentsContext, itemsCount, attachmentStreamCount, lastDocumentEtag, stats);

            OnDocumentsReceived(this);
        }

        private unsafe void ReceiveSingleIndexAndTransformersBatch(
            TransactionOperationContext configurationContext,
            int itemsCount,
            long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            var writeBuffer = configurationContext.GetStream();
            // this will read the indexes to memory from the network
            try
            {
                ReadIndexesTransformersFromSource(ref writeBuffer, itemsCount);
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
                writeBuffer.EnsureSingleChunk(out byte* buffer, out int totalSize);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {itemsCount:#,#;;0} indexes and transformers with size {totalSize / 1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");
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
                    ConflictsStorage.ConflictStatus conflictStatus;
                    using (configurationContext.OpenReadTransaction())
                        conflictStatus = GetConflictStatusForIndexOrTransformer(configurationContext, item.Name,
                            remote,
                            out conflictingVector);

                    ReadChangeVector(item, maxReceivedChangeVectorByDatabase);

                    using (var definition = new BlittableJsonReaderObject(buffer + item.Position, item.DefinitionSize, configurationContext))
                    {
                        switch (conflictStatus)
                        {
                            //note : PutIndexOrTransformer() is deleting conflicts and merges chnage vectors
                            //of the conflicts. This can be seen in IndexesEtagsStorage::WriteEntry()
                            case ConflictsStorage.ConflictStatus.Update:
                                PutIndexOrTransformer(configurationContext, item, definition);
                                break;
                            case ConflictsStorage.ConflictStatus.Conflict:
                                using (var txw = configurationContext.OpenWriteTransaction())
                                {
                                    HandleConflictForIndexOrTransformer(item, definition, conflictingVector, txw, configurationContext);

                                    UpdateIndexesChangeVector(txw, lastEtag, maxReceivedChangeVectorByDatabase);
                                    LastIndexOrTransformerEtag = lastEtag;
                                    txw.Commit();
                                    return; // skip the UpdateIndexesChangeVector below to avoid duplicate calls
                                }
                            case ConflictsStorage.ConflictStatus.AlreadyMerged:
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
                case IndexEntryType.Transformer:
                    //noop
                    break;
                case IndexEntryType.Index:
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
                    //noop
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
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

            _database.IndexStore.TryDeleteIndexIfExists(item.Name).Wait();
            try
            {
                IndexProcessor.Import(definition, _database, ServerVersion.BuildType, false);
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

        private unsafe void ReadIndexesTransformersFromSource(ref UnmanagedWriteBuffer writeBuffer, int itemsCount)
        {
            _replicatedIndexesAndTransformers.Clear();
            for (int x = 0; x < itemsCount; x++)
            {
                var curItem = new ReplicationIndexOrTransformerPositions
                {
                    Position = writeBuffer.SizeInBytes
                };

                var changeVectorCount = *(int*)ReadExactly(sizeof(int));

                var changeVectorSize = sizeof(ChangeVectorEntry) * changeVectorCount;
                ;
                curItem.ChangeVector = new ChangeVectorEntry[changeVectorCount];
                fixed (ChangeVectorEntry* pChangeVector = curItem.ChangeVector)
                    Memory.Copy((byte*)pChangeVector, ReadExactly(changeVectorSize), changeVectorSize);

                curItem.Etag = *(long*)ReadExactly(sizeof(long));

                int typeAsInt = *(int*)ReadExactly(sizeof(int));
                curItem.Type = (IndexEntryType)typeAsInt;

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

        private void ReadExactly(long size, FileStream file)
        {
            while (size > 0)
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
                var min = (int)Math.Min(size, available);
                file.Write(_connectionOptions.PinnedBuffer.Buffer.Array,
                    _connectionOptions.PinnedBuffer.Buffer.Offset + _connectionOptions.PinnedBuffer.Used,
                    min);
                _connectionOptions.PinnedBuffer.Used += min;
                size -= min;
            }
        }

        private unsafe void ReadExactly(int size, ref UnmanagedWriteBuffer into)
        {
            while (size > 0)
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

        private unsafe void ReceiveSingleDocumentsBatch(DocumentsOperationContext documentsContext, int replicatedItemsCount, int attachmentStreamCount, long lastEtag, IncomingReplicationStatsScope stats)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Receiving replication batch with {replicatedItemsCount} documents starting with {lastEtag} from {ConnectionInfo}");
            }

            var sw = Stopwatch.StartNew();
            var writeBuffer = documentsContext.GetStream();
            try
            {
                using (var networkStats = stats.For(ReplicationOperation.Incoming.Network))
                {
                    // this will read the documents to memory from the network
                    // without holding the write tx open
                    ReadItemsFromSource(ref writeBuffer, replicatedItemsCount, documentsContext, networkStats);

                    using (networkStats.For(ReplicationOperation.Incoming.AttachmentRead))
                    {
                        ReadAttachmentStreamsFromSource(ref writeBuffer, attachmentStreamCount, documentsContext);
                    }
                }

                writeBuffer.EnsureSingleChunk(out byte* buffer, out int totalSize);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {replicatedItemsCount:#,#;;0} documents with size {totalSize / 1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (stats.For(ReplicationOperation.Incoming.Storage))
                {
                    var replicationCommand = new MergedDocumentReplicationCommand(this, buffer, totalSize, lastEtag);
                    AsyncHelpers.RunSync(() => _database.TxMerger.Enqueue(replicationCommand));
                }

                sw.Stop();

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received and written {replicatedItemsCount:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastEtag}.");
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

        private void ReadChangeVector(ReplicationIndexOrTransformerPositions index, Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            for (int i = 0; i < index.ChangeVector.Length; i++)
            {
                if (maxReceivedChangeVectorByDatabase.TryGetValue(index.ChangeVector[i].DbId, out long etag) == false ||
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
            };

            documentsContext.Write(writer, heartbeat);

            writer.Flush();
            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;
        }

        public string SourceFormatted => $"{ConnectionInfo.SourceUrl}/databases/{ConnectionInfo.SourceDatabaseName} ({ConnectionInfo.SourceDatabaseId})";
        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private readonly ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationItem> _replicatedItems = new List<ReplicationItem>();
        private readonly Dictionary<Slice, ReplicationAttachmentStream> _replicatedAttachmentStreams = new Dictionary<Slice, ReplicationAttachmentStream>(SliceComparer.Instance);
        private readonly List<ReplicationIndexOrTransformerPositions> _replicatedIndexesAndTransformers = new List<ReplicationIndexOrTransformerPositions>();
        private long _lastDocumentEtag;
        private long _lastIndexOrTransformerEtag;
        private readonly TcpConnectionOptions _connectionOptions;
        private readonly ConflictManager _conflictManager;

        public struct ReplicationItem : IDisposable
        {
            public short TransactionMarker;
            public ReplicationBatchItem.ReplicationItemType Type;

            #region Document

            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public int DocumentSize;
            public string Collection;
            public long LastModifiedTicks;
            public DocumentFlags Flags;

            #endregion

            #region Attachment

            public Slice Key;
            public ByteStringContext.InternalScope KeyDispose;

            public Slice Name;
            public ByteStringContext.InternalScope NameDispose;

            public Slice ContentType;
            public ByteStringContext.InternalScope ContentTypeDispose;

            public Slice Base64Hash;
            public ByteStringContext.InternalScope Base64HashDispose;

            #endregion

            public void Dispose()
            {
                if (Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                {
                    KeyDispose.Dispose();
                    NameDispose.Dispose();
                    ContentTypeDispose.Dispose();
                    Base64HashDispose.Dispose();
                }
                else if (Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
                {
                    KeyDispose.Dispose();
                }
            }
        }

        public struct ReplicationAttachmentStream : IDisposable
        {
            public Slice Base64Hash;
            public IDisposable Base64HashDispose;

            public FileStream File;
            public IDisposable FileDispose;

            public void Dispose()
            {
                Base64HashDispose.Dispose();
                FileDispose.Dispose();
            }
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

        private unsafe void ReadItemsFromSource(ref UnmanagedWriteBuffer writeBuffer, int replicatedDocs, DocumentsOperationContext context, IncomingReplicationStatsScope stats)
        {
            var documentRead = stats.For(ReplicationOperation.Incoming.DocumentRead, start: false);
            var attachmentRead = stats.For(ReplicationOperation.Incoming.AttachmentRead, start: false);
            var tombstoneRead = stats.For(ReplicationOperation.Incoming.TombstoneRead, start: false);

            _replicatedItems.Clear();
            for (int x = 0; x < replicatedDocs; x++)
            {
                stats.RecordInputAttempt();

                var item = new ReplicationItem
                {
                    Type = *(ReplicationBatchItem.ReplicationItemType*)ReadExactly(sizeof(byte))
                };

                if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                {
                    stats.RecordAttachmentRead();

                    using (attachmentRead.Start())
                    {
                        item.TransactionMarker = *(short*)ReadExactly(sizeof(short));

                        var loweredKeySize = *(int*)ReadExactly(sizeof(int));
                        item.KeyDispose = Slice.From(context.Allocator, ReadExactly(loweredKeySize), loweredKeySize, out item.Key);

                        var nameSize = *(int*)ReadExactly(sizeof(int));
                        var name = Encoding.UTF8.GetString(ReadExactly(nameSize), nameSize);
                        item.NameDispose = DocumentKeyWorker.GetStringPreserveCase(context, name, out item.Name);

                        var contentTypeSize = *(int*)ReadExactly(sizeof(int));
                        var contentType = Encoding.UTF8.GetString(ReadExactly(contentTypeSize), contentTypeSize);
                        item.ContentTypeDispose = DocumentKeyWorker.GetStringPreserveCase(context, contentType, out item.ContentType);

                        var base64HashSize = *ReadExactly(sizeof(byte));
                        item.Base64HashDispose = Slice.From(context.Allocator, ReadExactly(base64HashSize), base64HashSize, out item.Base64Hash);
                    }
                }
                else if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
                {
                    stats.RecordAttachmentTombstoneRead();

                    using (tombstoneRead.Start())
                    {
                        item.TransactionMarker = *(short*)ReadExactly(sizeof(short));

                        var keySize = *(int*)ReadExactly(sizeof(int));
                        item.KeyDispose = Slice.From(context.Allocator, ReadExactly(keySize), keySize, out item.Key);
                    }
                }
                else
                {
                    IncomingReplicationStatsScope scope;

                    if (item.Type != ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
                    {
                        scope = documentRead;
                        stats.RecordDocumentRead();
                    }
                    else
                    {
                        scope = tombstoneRead;
                        stats.RecordDocumentTombstoneRead();
                    }

                    using (scope.Start())
                    {
                        item.Position = writeBuffer.SizeInBytes;
                        item.ChangeVectorCount = *(int*)ReadExactly(sizeof(int));

                        writeBuffer.Write(ReadExactly(sizeof(ChangeVectorEntry) * item.ChangeVectorCount), sizeof(ChangeVectorEntry) * item.ChangeVectorCount);

                        item.TransactionMarker = *(short*)ReadExactly(sizeof(short));

                        item.LastModifiedTicks = *(long*)ReadExactly(sizeof(long));

                        item.Flags = *(DocumentFlags*)ReadExactly(sizeof(DocumentFlags)) | DocumentFlags.FromReplication;

                        var keySize = *(int*)ReadExactly(sizeof(int));
                        item.Id = Encoding.UTF8.GetString(ReadExactly(keySize), keySize);

                        var documentSize = item.DocumentSize = *(int*)ReadExactly(sizeof(int));
                        if (documentSize != -1) //if -1, then this is a tombstone
                        {
                            ReadExactly(documentSize, ref writeBuffer);
                        }
                        else
                        {
                            //read the collection
                            var collectionSize = *(int*)ReadExactly(sizeof(int));
                            if (collectionSize != -1)
                            {
                                item.Collection = Encoding.UTF8.GetString(ReadExactly(collectionSize), collectionSize);
                            }
                        }
                    }
                }

                _replicatedItems.Add(item);
            }
        }

        private unsafe void ReadAttachmentStreamsFromSource(ref UnmanagedWriteBuffer writeBuffer, int attachmentStreamCount, DocumentsOperationContext context)
        {
            Debug.Assert(_replicatedAttachmentStreams.Count == 0, "We should handle all attachment streams during WriteAttachment.");

            for (int x = 0; x < attachmentStreamCount; x++)
            {
                var type = *(ReplicationBatchItem.ReplicationItemType*)ReadExactly(sizeof(byte));
                Debug.Assert(type == ReplicationBatchItem.ReplicationItemType.AttachmentStream);

                var attachment = new ReplicationAttachmentStream();

                var base64HashSize = *ReadExactly(sizeof(byte));
                attachment.Base64HashDispose = Slice.From(context.Allocator, ReadExactly(base64HashSize), base64HashSize, out attachment.Base64Hash);

                var streamLength = *(long*)ReadExactly(sizeof(long));
                attachment.FileDispose = _database.DocumentsStorage.AttachmentsStorage.GetTempFile(out attachment.File);
                ReadExactly(streamLength, attachment.File);

                attachment.File.Position = 0;
                _replicatedAttachmentStreams[attachment.Base64Hash] = attachment;
            }
        }

        private void AddReplicationPerformance(IncomingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out stats);
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

        private ConflictsStorage.ConflictStatus GetConflictStatusForIndexOrTransformer(TransactionOperationContext context, string name, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = _database.IndexMetadataPersistence.GetConflictsFor(context.Transaction.InnerTransaction, context, name, 0, int.MaxValue).ToList();
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    if (ConflictsStorage.GetConflictStatus(remote, existingConflict.ChangeVector) == ConflictsStorage.ConflictStatus.Conflict)
                    {
                        conflictingVector = existingConflict.ChangeVector;
                        return ConflictsStorage.ConflictStatus.Conflict;
                    }
                }

                return ConflictsStorage.ConflictStatus.Update;
            }

            var metadata = _database.IndexMetadataPersistence.GetIndexMetadataByName(context.Transaction.InnerTransaction, context, name, false);
            ChangeVectorEntry[] local;

            if (metadata != null)
                local = metadata.ChangeVector;
            else
                return ConflictsStorage.ConflictStatus.Update; //index/transformer with 'name' doesn't exist locally, so just do PUT


            var status = ConflictsStorage.GetConflictStatus(remote, local);
            if (status == ConflictsStorage.ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }

            return status;
        }

        private unsafe class MergedDocumentReplicationCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly IncomingReplicationHandler _incoming;

            private ChangeVectorEntry[] _changeVector = new ChangeVectorEntry[0];

            private readonly long _lastEtag;
            private readonly byte* _buffer;
            private readonly int _totalSize;

            public MergedDocumentReplicationCommand(IncomingReplicationHandler incoming, byte* buffer, int totalSize, long lastEtag)
            {
                _incoming = incoming;
                _buffer = buffer;
                _totalSize = totalSize;
                _lastEtag = lastEtag;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var operationsCount = 0;

                var database = _incoming._database;

                var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                foreach (var changeVectorEntry in database.DocumentsStorage.GetDatabaseChangeVector(context))
                {
                    maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                }

                foreach (var item in _incoming._replicatedItems)
                {
                    ++operationsCount;
                    using (item)
                    {
                        Debug.Assert(item.Flags.HasFlag(DocumentFlags.Artificial) == false);

                        if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                        {
                            if (_incoming._log.IsInfoEnabled)
                                _incoming._log.Info($"Got incoming attachment, doing PUT on attachment = {item.Name}, with key = {item.Key}");

                            database.DocumentsStorage.AttachmentsStorage.PutFromReplication(context, item.Key, item.Name,
                                item.ContentType, item.Base64Hash, item.TransactionMarker);

                            if (_incoming._replicatedAttachmentStreams.TryGetValue(item.Base64Hash, out ReplicationAttachmentStream attachmentStream))
                            {
                                using (attachmentStream)
                                {
                                    if (_incoming._log.IsInfoEnabled)
                                        _incoming._log.Info($"Got incoming attachment stream, doing PUT on attachment stream = {attachmentStream.Base64Hash}");

                                    database.DocumentsStorage.AttachmentsStorage.
                                        PutAttachmentStream(context, item.Key, attachmentStream.Base64Hash, attachmentStream.File);
                                }
                                _incoming._replicatedAttachmentStreams.Remove(item.Base64Hash);
                            }
                        }
                        else if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
                        {
                            if (_incoming._log.IsInfoEnabled)
                                _incoming._log.Info($"Got incoming attachment tombstone, doing DELETE on attachment {item.Key}");

                            database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, item.Key, false, "$fromReplication", null);
                        }
                        else
                        {
                            context.TransactionMarkerOffset = item.TransactionMarker;
                            BlittableJsonReaderObject document = null;
                            try
                            {
                                ReadChangeVector(item.ChangeVectorCount, item.Position, _buffer, maxReceivedChangeVectorByDatabase);
                                if (item.DocumentSize >= 0) //no need to load document data for tombstones
                                                            // document size == -1 --> doc is a tombstone
                                {
                                    if (item.Position + item.DocumentSize > _totalSize)
                                        throw new ArgumentOutOfRangeException($"Reading past the size of buffer! TotalSize {_totalSize} " +
                                                                              $"but position is {item.Position} & size is {item.DocumentSize}!");

                                    //if something throws at this point, this means something is really wrong and we should stop receiving documents.
                                    //the other side will receive negative ack and will retry sending again.
                                    document = new BlittableJsonReaderObject(
                                        _buffer + item.Position + (item.ChangeVectorCount * sizeof(ChangeVectorEntry)),
                                        item.DocumentSize, context);
                                    document.BlittableValidation();
                                }

                                if ((item.Flags & DocumentFlags.Revision) == DocumentFlags.Revision)
                                {
                                    if (database.BundleLoader.VersioningStorage == null)
                                    {
                                        if (_incoming._log.IsOperationsEnabled)
                                            _incoming._log.Operations("Versioing storage is disabled but the node got a versioned document from replication.");
                                        continue;
                                    }
                                    database.BundleLoader.VersioningStorage.PutFromDocument(context, item.Id, document, item.Flags, _changeVector, item.LastModifiedTicks);
                                    continue;
                                }

                                ChangeVectorEntry[] conflictingVector;
                                var conflictStatus = ConflictsStorage.GetConflictStatusForDocument(context, item.Id, _changeVector, out conflictingVector);

                                switch (conflictStatus)
                                {
                                    case ConflictsStorage.ConflictStatus.Update:
                                        if (document != null)
                                        {
                                            if (_incoming._log.IsInfoEnabled)
                                                _incoming._log.Info(
                                                    $"Conflict check resolved to Update operation, doing PUT on doc = {item.Id}, with change vector = {_changeVector.Format()}");
                                            database.DocumentsStorage.Put(context, item.Id, null, document, item.LastModifiedTicks, _changeVector,
                                                item.Flags, NonPersistentDocumentFlags.FromReplication);
                                        }
                                        else
                                        {
                                            if (_incoming._log.IsInfoEnabled)
                                                _incoming._log.Info(
                                                    $"Conflict check resolved to Update operation, writing tombstone for doc = {item.Id}, with change vector = {_changeVector.Format()}");
                                            using (DocumentKeyWorker.GetSliceFromKey(context, item.Id, out Slice keySlice))
                                            {
                                                database.DocumentsStorage.Delete(
                                                    context, keySlice, item.Id, null,
                                                    item.LastModifiedTicks,
                                                    _changeVector,
                                                    context.GetLazyString(item.Collection));
                                            }
                                        }
                                        break;
                                    case ConflictsStorage.ConflictStatus.Conflict:
                                        if (_incoming._log.IsInfoEnabled)
                                            _incoming._log.Info(
                                                $"Conflict check resolved to Conflict operation, resolving conflict for doc = {item.Id}, with change vector = {_changeVector.Format()}");
                                        _incoming._conflictManager.HandleConflictForDocument(context, item.Id, item.Collection, item.LastModifiedTicks, document, _changeVector, conflictingVector);
                                        break;
                                    case ConflictsStorage.ConflictStatus.AlreadyMerged:
                                        if (_incoming._log.IsInfoEnabled)
                                            _incoming._log.Info(
                                                $"Conflict check resolved to AlreadyMerged operation, nothing to do for doc = {item.Id}, with change vector = {_changeVector.Format()}");
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
                    }
                }

                Debug.Assert(_incoming._replicatedAttachmentStreams.Count == 0, "We should handle all attachment streams during WriteAttachment.");

                database.DocumentsStorage.SetDatabaseChangeVector(context, maxReceivedChangeVectorByDatabase);
                database.DocumentsStorage.SetLastReplicateEtagFrom(context, _incoming.ConnectionInfo.SourceDatabaseId, _lastEtag);

                return operationsCount;
            }

            private void ReadChangeVector(int changeVectorCount, int position,
                byte* buffer, Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
            {
                if (_changeVector.Length != changeVectorCount)
                    _changeVector = new ChangeVectorEntry[changeVectorCount];

                for (int i = 0; i < changeVectorCount; i++)
                {
                    _changeVector[i] = ((ChangeVectorEntry*)(buffer + position))[i];

                    if (maxReceivedChangeVectorByDatabase.TryGetValue(_changeVector[i].DbId, out long etag) == false ||
    etag > _changeVector[i].Etag)
                    {
                        maxReceivedChangeVectorByDatabase[_changeVector[i].DbId] = _changeVector[i].Etag;
                    }
                }
            }
        }
    }
}
