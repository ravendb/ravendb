using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Text;
using Sparrow;
using Sparrow.Json.Parsing;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron;
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
        public event Action<LiveReplicationPulsesCollector.ReplicationPulse> HandleReplicationPulse;

        public long LastDocumentEtag;
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

            _attachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("replication");
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

        private string IncomingReplicationThreadName => $"Incoming replication {FromToString}";

        public void Start()
        {
            if (_incomingThread != null)
                return;

            var result = Interlocked.CompareExchange(ref _incomingThread, new Thread(ReceiveReplicationBatches)
            {
                IsBackground = true,
                Name = IncomingReplicationThreadName
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
        public static bool IsIncomingReplication;

        private readonly AsyncManualResetEvent _replicationFromAnotherSource = new AsyncManualResetEvent();

        public void OnReplicationFromAnotherSource()
        {
            _replicationFromAnotherSource.Set();
        }

        private void ReceiveReplicationBatches()
        {
            NativeMemory.EnsureRegistered();
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
                            AddReplicationPulse(ReplicationPulseDirection.IncomingInitiate);

                            using (var msg = interruptibleRead.ParseToMemory(
                                _replicationFromAnotherSource,
                                "IncomingReplication/read-message",
                                Timeout.Infinite,
                                _connectionOptions.PinnedBuffer,
                                _database.DatabaseShutdown))
                            {
                                if (msg.Document != null)
                                {
                                    using (var writer = new BlittableJsonTextWriter(msg.Context, _stream))
                                    {
                                        HandleSingleReplicationBatch(msg.Context,
                                            msg.Document,
                                            writer);
                                    }
                                }
                                else // notify peer about new change vector
                                {
                                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(
                                            out DocumentsOperationContext documentsContext))
                                    using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
                                    {
                                        SendHeartbeatStatusToSource(
                                            documentsContext,
                                            writer,
                                            _lastDocumentEtag,
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
                            AddReplicationPulse(ReplicationPulseDirection.IncomingInitiateError, e.Message);

                            if (_log.IsInfoEnabled)
                            {
                                if (e.InnerException is SocketException)
                                {
                                    if (_log.IsInfoEnabled)
                                        _log.Info("Failed to read data from incoming connection. The incoming connection will be closed and re-created.", e);
                                }
                                else
                                {
                                    //if we are disposing, do not notify about failure (not relevant)
                                    if (_cts.IsCancellationRequested == false)
                                        if (_log.IsInfoEnabled)
                                            _log.Info("Received unexpected exception while receiving replication batch.",e);
                                }
                            }

                            throw;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (_cts.IsCancellationRequested == false)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.", e);

                    OnFailed(e, this);
                }
            }
        }

        private Task _prevChangeVectorUpdate;

        private void HandleSingleReplicationBatch(
            DocumentsOperationContext documentsContext,
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
                

                switch (messageType)
                {
                    case ReplicationMessageType.Documents:
                        AddReplicationPulse(ReplicationPulseDirection.IncomingBegin);

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
                                    AddReplicationPulse(ReplicationPulseDirection.IncomingError, e.Message);
                                    scope.AddError(e);
                                    throw;
                                }
                            }
                        }
                        finally
                        {
                            AddReplicationPulse(ReplicationPulseDirection.IncomingEnd);
                            stats.Complete();
                        }
                    case ReplicationMessageType.Heartbeat:
                        AddReplicationPulse(ReplicationPulseDirection.IncomingHeartbeat);
                        if (message.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector))
                        {
                            var cmd = new MergedUpdateDatabaseChangeVectorCommand(changeVector, _replicationFromAnotherSource);
                            if (_prevChangeVectorUpdate != null && _prevChangeVectorUpdate.IsCompleted == false)
                            {
                                if (_log.IsInfoEnabled)
                                {
                                    _log.Info(
                                        $"The previous task of updating the database change vector was not completed and has the status of {_prevChangeVectorUpdate.Status}, " +
                                        "nevertheless we create an additional task.");
                                }
                            }
                            else
                            {
                                _prevChangeVectorUpdate = _database.TxMerger.Enqueue(cmd).AsTask();
                            }
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown message type: " + messageType);
                }
                SendHeartbeatStatusToSource(documentsContext, writer, _lastDocumentEtag,  messageType);
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

        private void ReadExactly(long size, Stream file)
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
            UnmanagedMemory.Move(
                _connectionOptions.PinnedBuffer.Pointer,
                _connectionOptions.PinnedBuffer.Pointer + _connectionOptions.PinnedBuffer.Used,
                diff);
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
            Task task = null;
            try
            {
                using (var networkStats = stats.For(ReplicationOperation.Incoming.Network))
                {
                    // this will read the documents to memory from the network
                    // without holding the write tx open
                    ReadItemsFromSource(ref writeBuffer, replicatedItemsCount, documentsContext, networkStats);

                    using (networkStats.For(ReplicationOperation.Incoming.AttachmentRead))
                    {
                        ReadAttachmentStreamsFromSource(attachmentStreamCount, documentsContext);
                    }
                }

                writeBuffer.EnsureSingleChunk(out byte* buffer, out int totalSize);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {replicatedItemsCount:#,#;;0} documents with size {totalSize / 1024:#,#;;0} kb to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (stats.For(ReplicationOperation.Incoming.Storage))
                {
                    var replicationCommand = new MergedDocumentReplicationCommand(this, buffer, totalSize, lastEtag);
                    task = _database.TxMerger.Enqueue(replicationCommand).AsTask();

                    using (var writer = new BlittableJsonTextWriter(documentsContext, _connectionOptions.Stream))
                    using (var msg = documentsContext.ReadObject(new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.MessageType)] = "Processing"
                    }, "heartbeat message"))
                    {
                        while (task.Wait(Math.Min(3000, (int)(_database.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds * 2 / 3))) == false)
                        {
                            // send heartbeats while batch is processed in TxMerger. We wait until merger finishes with this command without timeouts
                            documentsContext.Write(writer, msg);
                            writer.Flush();
                        }
                        task = null;
                    }
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
                // before we dispose the buffer we must ensure it is not being processed in TxMerger, so we wait for it
                try
                {
                    task?.Wait();
                }
                catch (Exception)
                {
                    // ignore this failure, if this failed, we are already
                    // in a bad state and likely in the process of shutting 
                    // down
                }
                writeBuffer.Dispose();
            }
        }

        private void SendHeartbeatStatusToSource(DocumentsOperationContext documentsContext, BlittableJsonTextWriter writer, long lastDocumentEtag, string handledMessageType)
        {
            AddReplicationPulse(ReplicationPulseDirection.IncomingHeartbeatAcknowledge);

            string databaseChangeVector;
            long currentLastEtagMatchingChangeVector;

            using (documentsContext.OpenReadTransaction())
            {
                // we need to get both of them in a transaction, the other side will check if its known change vector
                // is the same or higher then ours, and if so, we'll update the change vector on the sibling to reflect
                // our own latest etag. This allows us to have effective syncronzation points, since each change will
                // be able to tell (roughly) where it is at on the entire cluster. 
                databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(documentsContext);
                currentLastEtagMatchingChangeVector = DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction);
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"Sending heartbeat ok => {FromToString} with last document etag = {lastDocumentEtag}, last document change vector: {databaseChangeVector}");
            }
            var heartbeat = new DynamicJsonValue
            {
                [nameof(ReplicationMessageReply.Type)] = "Ok",
                [nameof(ReplicationMessageReply.MessageType)] = handledMessageType,
                [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastDocumentEtag,
                [nameof(ReplicationMessageReply.CurrentEtag)] = currentLastEtagMatchingChangeVector,
                [nameof(ReplicationMessageReply.Exception)] = null,
                [nameof(ReplicationMessageReply.DatabaseChangeVector)] = databaseChangeVector,
                [nameof(ReplicationMessageReply.DatabaseId)] = _database.DbId.ToString(),
                [nameof(ReplicationMessageReply.NodeTag)] = _parent._server.NodeTag

            };

            documentsContext.Write(writer, heartbeat);

            writer.Flush();
            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;
        }

        public string SourceFormatted => $"{ConnectionInfo.SourceUrl}/databases/{ConnectionInfo.SourceDatabaseName} ({ConnectionInfo.SourceDatabaseId})";
        public string FromToString => $"from {ConnectionInfo.SourceTag}-{ConnectionInfo.SourceDatabaseName} @ {ConnectionInfo.SourceUrl} " +
                                      $"into database {_database.ServerStore.NodeTag}-{_database.Name} @ {_database.ServerStore.NodeTcpServerUrl}";
        public IncomingConnectionInfo ConnectionInfo { get; }
        
        private readonly List<ReplicationItem> _replicatedItems = new List<ReplicationItem>();
        private readonly StreamsTempFile _attachmentStreamsTempFile;
        private readonly Dictionary<Slice, ReplicationAttachmentStream> _replicatedAttachmentStreams = new Dictionary<Slice, ReplicationAttachmentStream>(SliceComparer.Instance);
        private long _lastDocumentEtag;
        private readonly TcpConnectionOptions _connectionOptions;
        private readonly ConflictManager _conflictManager;

        private struct ReplicationItem : IDisposable
        {
            public short TransactionMarker;
            public ReplicationBatchItem.ReplicationItemType Type;

            #region Document

            public string Id;
            public int Position;
            public string ChangeVector;
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
                else if (Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone ||
                         Type == ReplicationBatchItem.ReplicationItemType.RevisionTombstone)
                {
                    KeyDispose.Dispose();
                }
            }
        }

        private struct ReplicationAttachmentStream : IDisposable
        {
            public Slice Base64Hash;
            public ByteStringContext.InternalScope Base64HashDispose;

            public Stream Stream;

            public void Dispose()
            {
                Base64HashDispose.Dispose();
                Stream.Dispose();
            }
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
                    Type = *(ReplicationBatchItem.ReplicationItemType*)ReadExactly(sizeof(byte)),
                    Position = writeBuffer.SizeInBytes
                };

                var changeVectorSize = *(int*)ReadExactly(sizeof(int));
                item.ChangeVector = Encoding.UTF8.GetString(ReadExactly(changeVectorSize), changeVectorSize);

                item.TransactionMarker = *(short*)ReadExactly(sizeof(short));

                if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                {
                    stats.RecordAttachmentRead();

                    using (attachmentRead.Start())
                    {
                        var loweredKeySize = *(int*)ReadExactly(sizeof(int));
                        item.KeyDispose = Slice.From(context.Allocator, ReadExactly(loweredKeySize), loweredKeySize, out item.Key);

                        var nameSize = *(int*)ReadExactly(sizeof(int));
                        var name = Encoding.UTF8.GetString(ReadExactly(nameSize), nameSize);
                        item.NameDispose = DocumentIdWorker.GetStringPreserveCase(context, name, out item.Name);

                        var contentTypeSize = *(int*)ReadExactly(sizeof(int));
                        var contentType = Encoding.UTF8.GetString(ReadExactly(contentTypeSize), contentTypeSize);
                        item.ContentTypeDispose = DocumentIdWorker.GetStringPreserveCase(context, contentType, out item.ContentType);

                        var base64HashSize = *ReadExactly(sizeof(byte));
                        item.Base64HashDispose = Slice.From(context.Allocator, ReadExactly(base64HashSize), base64HashSize, out item.Base64Hash);
                    }
                }
                else if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
                {
                    stats.RecordAttachmentTombstoneRead();

                    using (tombstoneRead.Start())
                    {
                        var keySize = *(int*)ReadExactly(sizeof(int));
                        item.KeyDispose = Slice.From(context.Allocator, ReadExactly(keySize), keySize, out item.Key);
                    }
                }
                else if (item.Type == ReplicationBatchItem.ReplicationItemType.RevisionTombstone)
                {
                    stats.RecordRevisionTombstoneRead();

                    using (tombstoneRead.Start())
                    {
                        var keySize = *(int*)ReadExactly(sizeof(int));
                        item.KeyDispose = Slice.From(context.Allocator, ReadExactly(keySize), keySize, out item.Key);

                        var collectionSize = *(int*)ReadExactly(sizeof(int));
                        Debug.Assert(collectionSize > 0);
                        item.Collection = Encoding.UTF8.GetString(ReadExactly(collectionSize), collectionSize);
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
                            // read the collection
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

        private unsafe void ReadAttachmentStreamsFromSource(int attachmentStreamCount, DocumentsOperationContext context)
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
                attachment.Stream = _attachmentStreamsTempFile.StartNewStream();
                ReadExactly(streamLength, attachment.Stream);

                _replicatedAttachmentStreams[attachment.Base64Hash] = attachment;
            }
        }

        private void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null)
        {
            HandleReplicationPulse?.Invoke(new LiveReplicationPulsesCollector.ReplicationPulse
            {
                OccurredAt = SystemTime.UtcNow,
                Direction = direction,
                From = ConnectionInfo,
                ExceptionMessage = exceptionMessage
            });
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

            _attachmentStreamsTempFile.Dispose();
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);

        private class MergedUpdateDatabaseChangeVectorCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly string _changeVector;
            private readonly AsyncManualResetEvent _trigger;

            public MergedUpdateDatabaseChangeVectorCommand(string chageVector, AsyncManualResetEvent trigger)
            {
                _changeVector = chageVector;
                _trigger = trigger;
            }
            public override int Execute(DocumentsOperationContext context)
            {
                var current = DocumentsStorage.GetDatabaseChangeVector(context);
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(_changeVector, current);
                if (conflictStatus == ConflictStatus.Update)
                {
                    var merged = ChangeVectorUtils.MergeVectors(current, _changeVector);
                    DocumentsStorage.SetDatabaseChangeVector(context, merged);
                    context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                    {
                        try
                        {
                            _trigger.Set();
                        }
                        catch
                        {
                            //
                        }
                    };
                    return 1;
                }
                return 0;
            }
        }

        private unsafe class MergedDocumentReplicationCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly IncomingReplicationHandler _incoming;

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
                try
                {
                    IsIncomingReplication = true;

                    var operationsCount = 0;

                    var database = _incoming._database;

                    var currentDatabaseChangeVector = context.LastDatabaseChangeVector ??
                               (context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context));

                    var maxReceivedChangeVectorByDatabase = currentDatabaseChangeVector;

                    foreach (var item in _incoming._replicatedItems)
                    {
                        context.TransactionMarkerOffset = item.TransactionMarker;

                        ++operationsCount;
                        using (var rcvdChangeVector = context.GetLazyString(item.ChangeVector))
                        using (item)
                        {
                            Debug.Assert(item.Flags.Contain(DocumentFlags.Artificial) == false);

                           
                            maxReceivedChangeVectorByDatabase =
                                ChangeVectorUtils.MergeVectors(item.ChangeVector, maxReceivedChangeVectorByDatabase);

                            if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                            {
                                if (_incoming._log.IsInfoEnabled)
                                    _incoming._log.Info($"AttachmentPUT '{item.Name}' - '{item.Key}', with change vector = {item.ChangeVector}");

                                database.DocumentsStorage.AttachmentsStorage.PutDirect(context, item.Key, item.Name,
                                    item.ContentType, item.Base64Hash, rcvdChangeVector);

                                if (_incoming._replicatedAttachmentStreams.TryGetValue(item.Base64Hash, out ReplicationAttachmentStream attachmentStream))
                                {
                                    using (attachmentStream)
                                    {
                                        if (_incoming._log.IsInfoEnabled)
                                            _incoming._log.Info($"AttachmentStreamPUT '{attachmentStream.Base64Hash}' - '{item.Name}' - '{item.Key}', with change vector = {item.ChangeVector}");

                                        database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, item.Key, attachmentStream.Base64Hash, attachmentStream.Stream);

                                        _incoming._replicatedAttachmentStreams.Remove(item.Base64Hash);
                                    }
                                }
                            }
                            else if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
                            {
                                if (_incoming._log.IsInfoEnabled)
                                    _incoming._log.Info($"AttachmentDELETE '{item.Key}', with change vector = {item.ChangeVector}");
                                database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, item.Key, false, "$fromReplication", null, rcvdChangeVector);
                            }
                            else if (item.Type == ReplicationBatchItem.ReplicationItemType.RevisionTombstone)
                            {
                                if (_incoming._log.IsInfoEnabled)
                                    _incoming._log.Info($"RevisionDELETE '{item.Key}', with change vector = {item.ChangeVector}");
                                database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, item.Key, item.Collection, rcvdChangeVector);
                            }
                            else
                            {
                                BlittableJsonReaderObject document = null;
                                try
                                {
                                    // no need to load document data for tombstones
                                    // document size == -1 --> doc is a tombstone
                                    if (item.DocumentSize >= 0)
                                    {
                                        if (item.Position + item.DocumentSize > _totalSize)
                                            throw new ArgumentOutOfRangeException($"Reading past the size of buffer! TotalSize {_totalSize} " +
                                                                                  $"but position is {item.Position} & size is {item.DocumentSize}!");

                                        //if something throws at this point, this means something is really wrong and we should stop receiving documents.
                                        //the other side will receive negative ack and will retry sending again.
                                        document = new BlittableJsonReaderObject(_buffer + item.Position, item.DocumentSize, context);
                                        document.BlittableValidation();
                                    }

                                    if ((item.Flags & DocumentFlags.Revision) == DocumentFlags.Revision)
                                    {
                                        if (database.DocumentsStorage.RevisionsStorage.Configuration == null
                                            && item.Flags.Contain(DocumentFlags.Resolved) == false
                                            && item.Flags.Contain(DocumentFlags.Conflicted) == false)
                                        {
                                            if (_incoming._log.IsOperationsEnabled)
                                                _incoming._log.Operations("Revisions are disabled but the node got a revision from replication.");
                                            continue;
                                        }
                                        if (_incoming._log.IsInfoEnabled)
                                            _incoming._log.Info($"RevisionPUT '{item.Id}', with change vector = {item.ChangeVector}");
                                        database.DocumentsStorage.RevisionsStorage.Put(context, item.Id, document, item.Flags,
                                            NonPersistentDocumentFlags.FromReplication, rcvdChangeVector, item.LastModifiedTicks);
                                        continue;
                                    }

                                    if ((item.Flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision)
                                    {
                                        if (database.DocumentsStorage.RevisionsStorage.Configuration == null)
                                        {
                                            if (_incoming._log.IsOperationsEnabled)
                                                _incoming._log.Operations("Revisions are disabled but the node got a delete revision from replication.");
                                            continue;
                                        }
                                        if (_incoming._log.IsInfoEnabled)
                                            _incoming._log.Info($"RevisionDELETE '{item.Id}', with change vector = {item.ChangeVector}");
                                        database.DocumentsStorage.RevisionsStorage.Delete(context, item.Id, document, rcvdChangeVector,
                                            item.LastModifiedTicks, NonPersistentDocumentFlags.FromReplication, item.Flags);
                                        continue;
                                    }

                                    var conflictStatus = ConflictsStorage.GetConflictStatusForDocument(context, item.Id, rcvdChangeVector,
                                        out var conflictingVector);
                                    switch (conflictStatus)
                                    {
                                        case ConflictStatus.Update:
                                            if (document != null)
                                            {
                                                if (_incoming._log.IsInfoEnabled)
                                                    _incoming._log.Info($"Conflict check resolved to Update operation, doing PUT on doc = {item.Id}, " +
                                                                        $"with change vector = {item.ChangeVector}");
#if DEBUG
                                                AttachmentsStorage.AssertAttachments(document, item.Flags);
#endif
                                                database.DocumentsStorage.Put(context, item.Id, null, document, item.LastModifiedTicks, rcvdChangeVector,
                                                    item.Flags, NonPersistentDocumentFlags.FromReplication);
                                            }
                                            else
                                            {
                                                if (_incoming._log.IsInfoEnabled)
                                                    _incoming._log.Info(
                                                        $"Conflict check resolved to Update operation, writing tombstone for doc = {item.Id}, " +
                                                        $"with change vector = {item.ChangeVector}");
                                                using (DocumentIdWorker.GetSliceFromId(context, item.Id, out Slice keySlice))
                                                {
                                                    database.DocumentsStorage.Delete(
                                                        context, keySlice, item.Id, null,
                                                        item.LastModifiedTicks,
                                                        rcvdChangeVector,
                                                        new CollectionName(item.Collection),
                                                        NonPersistentDocumentFlags.FromReplication);
                                                }
                                            }
                                            break;
                                        case ConflictStatus.Conflict:
                                            if (_incoming._log.IsInfoEnabled)
                                                _incoming._log.Info($"Conflict check resolved to Conflict operation, resolving conflict for doc = {item.Id}, with change vector = {item.ChangeVector}");
                                            // if the conflict is going to ber resolved locally, that means that we have local work to do
                                            // that we need to distribute to our siblings
                                            IsIncomingReplication = false;
                                            _incoming._conflictManager.HandleConflictForDocument(context, item.Id, item.Collection, item.LastModifiedTicks, document, rcvdChangeVector, conflictingVector, item.Flags);
                                            break;
                                        case ConflictStatus.AlreadyMerged:
                                            if (_incoming._log.IsInfoEnabled)
                                                _incoming._log.Info(
                                                    $"Conflict check resolved to AlreadyMerged operation, nothing to do for doc = {item.Id}, with change vector = {item.ChangeVector}");
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

                    _incoming._attachmentStreamsTempFile.Reset();

                    Debug.Assert(_incoming._replicatedAttachmentStreams.Count == 0, "We should handle all attachment streams during WriteAttachment.");
                    Debug.Assert(context.LastDatabaseChangeVector != null);

                    // instead of : SetDatabaseChangeVector -> maxReceivedChangeVectorByDatabase , we will store in context and write once right before commit (one time instead of repeating on all docs in the same Tx)
                    context.LastDatabaseChangeVector = maxReceivedChangeVectorByDatabase;

                    // instead of : SetLastReplicateEtagFrom -> _incoming.ConnectionInfo.SourceDatabaseId, _lastEtag , we will store in context and write once right before commit (one time instead of repeating on all docs in the same Tx)
                    if (context.LastReplicationEtagFrom == null)
                        context.LastReplicationEtagFrom = new Dictionary<string, long>();
                    context.LastReplicationEtagFrom[_incoming.ConnectionInfo.SourceDatabaseId] = _lastEtag;
                    return operationsCount;
                }
                finally
                {
                    IsIncomingReplication = false;
                }
            }
        }
    }
}
