using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract class AbstractIncomingReplicationHandler<TContextPool, TOperationContext> : IDisposable
    where TContextPool : JsonContextPoolBase<TOperationContext>
    where TOperationContext : JsonOperationContext
    {
        private PoolOfThreads.LongRunningWork _incomingWork;
        private readonly DisposeOnce<SingleAttempt> _disposeOnce;
        private readonly ServerStore _server;
        private IDisposable _connectionOptionsDisposable;
        private readonly TContextPool _contextPool;
        private readonly string _databaseName;
        private readonly ConcurrentQueue<IncomingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<IncomingReplicationStatsAggregator>();
        private IncomingReplicationStatsAggregator _lastStats;
        private readonly TcpClient _tcpClient;

        protected readonly Stream _stream;
        protected readonly TcpConnectionOptions _tcpConnectionOptions;
        protected readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;
        protected readonly CancellationTokenSource _cts;
        protected StreamsTempFile _attachmentStreamsTempFile;
        protected long _lastDocumentEtag;
        protected AsyncManualResetEvent _replicationFromAnotherSource;

        protected Logger Logger;

        public IncomingConnectionInfo ConnectionInfo { get; protected set; }
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; set; }
        protected string IncomingReplicationThreadName => $"Incoming replication {FromToString}";
        public virtual string FromToString => $"In database {_server.NodeTag}-{_databaseName} @ {_server.GetNodeTcpServerUrl()} " +
                                              $"from {ConnectionInfo.SourceTag}-{ConnectionInfo.SourceDatabaseName} @ {ConnectionInfo.SourceUrl}";

        protected AbstractIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer,
            ServerStore server, string databaseName, ReplicationLatestEtagRequest replicatedLastEtag, CancellationToken token, TContextPool contextPool)
        {
            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);
            _tcpConnectionOptions = tcpConnectionOptions;
            _copiedBuffer = buffer.Clone(_tcpConnectionOptions.ContextPool);

            _tcpClient = tcpConnectionOptions.TcpClient;
            _stream = tcpConnectionOptions.Stream;
            _server = server;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _databaseName = databaseName;
            _contextPool = contextPool;

            Logger = LoggingSource.Instance.GetLogger(databaseName, GetType().FullName);

            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(tcpConnectionOptions.Operation, tcpConnectionOptions.ProtocolVersion);
            ConnectionInfo.RemoteIp = ((IPEndPoint)_tcpClient.Client.RemoteEndPoint)?.Address.ToString();
        }

        public void Start()
        {
            if (_incomingWork != null)
                return;

            lock (this)
            {
                if (_incomingWork != null)
                    return; // already set by someone else, they can start it

                _incomingWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => { DoIncomingReplication(); }, null, IncomingReplicationThreadName);
            }

            if (Logger.IsInfoEnabled)
                Logger.Info($"Incoming replication thread started ({FromToString})");
        }

        public void DoIncomingReplication()
        {
            try
            {
                ReceiveReplicationBatches();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Error in accepting replication request ({FromToString})", e);
            }
        }

        protected void ReceiveReplicationBatches()
        {
            NativeMemory.EnsureRegistered();
            try
            {
                using (_connectionOptionsDisposable = _tcpConnectionOptions.ConnectionProcessingInProgress("Replication"))
                using (_stream)
                using (var interruptibleRead = new InterruptibleRead<TContextPool, TOperationContext>(_contextPool, _stream))
                {
                    while (_cts.IsCancellationRequested == false)
                    {
                        try
                        {
                            AddReplicationPulse(ReplicationPulseDirection.IncomingInitiate);

                            using (var msg = interruptibleRead.ParseToMemory(
                                _replicationFromAnotherSource,
                                "IncomingReplication/read-message",
                                Timeout.Infinite,
                                _copiedBuffer.Buffer,
                                _cts.Token))
                            {
                                if (msg.Document != null)
                                {
                                    EnsureNotDeleted(_server.NodeTag);

                                    using (var writer = new BlittableJsonTextWriter(msg.Context, _stream))
                                    {
                                        HandleSingleReplicationBatch(msg.Context,
                                            msg.Document,
                                            writer);
                                    }
                                }
                                else // notify peer about new change vector
                                {
                                    using (_contextPool.AllocateOperationContext(out TOperationContext context))
                                    using (var writer = new BlittableJsonTextWriter(context, _stream))
                                    {
                                        SendHeartbeatStatusToSource(
                                            context,
                                            writer,
                                            _lastDocumentEtag,
                                            "Notify");
                                    }
                                }
                                // we reset it after every time we send to the remote server
                                // because that is when we know that it is up to date with our
                                // status, so no need to send again
                                _replicationFromAnotherSource?.Reset();
                            }
                        }
                        catch (Exception e)
                        {
                            AddReplicationPulse(ReplicationPulseDirection.IncomingInitiateError, e.Message);

                            if (Logger.IsInfoEnabled)
                            {
                                if (e is AggregateException ae &&
                                    ae.InnerExceptions.Count == 1 &&
                                    ae.InnerException is SocketException ase)
                                {
                                    HandleSocketException(ase);
                                }
                                else if (e.InnerException is SocketException se)
                                {
                                    HandleSocketException(se);
                                }
                                else
                                {
                                    //if we are disposing, do not notify about failure (not relevant)
                                    if (_cts.IsCancellationRequested == false)
                                        if (Logger.IsInfoEnabled)
                                            Logger.Info("Received unexpected exception while receiving replication batch.", e);
                                }
                            }

                            throw;
                        }

                        void HandleSocketException(SocketException e)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Failed to read data from incoming connection. The incoming connection will be closed and re-created.", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (_cts.IsCancellationRequested == false)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.", e);

                    OnFailed(e);
                }
            }
        }

        internal void HandleSingleReplicationBatch(
    TOperationContext context,
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
                    throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

                if (!message.TryGet(nameof(ReplicationMessageHeader.LastDocumentEtag), out _lastDocumentEtag))
                    throw new InvalidOperationException("Expected LastDocumentEtag property in the replication message, " +
                                                        "but didn't find it..");

                switch (messageType)
                {
                    case ReplicationMessageType.Documents:
                        AddReplicationPulse(ReplicationPulseDirection.IncomingBegin);

                        var stats = _lastStats = new IncomingReplicationStatsAggregator(GetNextReplicationStatsId(), _lastStats);
                        AddReplicationPerformance(stats);

                        try
                        {
                            using (var scope = stats.CreateScope())
                            {
                                try
                                {
                                    scope.RecordLastEtag(_lastDocumentEtag);

                                    HandleReceivedDocumentsAndAttachmentsBatch(context, message, _lastDocumentEtag, scope);
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

                        HandleHeartbeatMessage(context, message);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("Unknown message type: " + messageType);
                }

                SendHeartbeatStatusToSource(context, writer, _lastDocumentEtag, messageType);
            }
            catch (ObjectDisposedException)
            {
                //we are shutting down replication, this is ok
            }
            catch (EndOfStreamException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Received unexpected end of stream while receiving replication batches. " +
                              "This might indicate an issue with network.", e);
                throw;
            }
            catch (Exception e)
            {
                //if we are disposing, ignore errors
                if (_cts.IsCancellationRequested)
                    return;

                DynamicJsonValue returnValue;

                if (e.ExtractSingleInnerException() is MissingAttachmentException mae)
                {
                    returnValue = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = ReplicationMessageReply.ReplyType.MissingAttachments.ToString(),
                        [nameof(ReplicationMessageReply.MessageType)] = messageType,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = -1,
                        [nameof(ReplicationMessageReply.Exception)] = mae.ToString()
                    };

                    context.Write(writer, returnValue);
                    writer.Flush();

                    return;
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed replicating documents {FromToString}.", e);

                //return negative ack
                returnValue = new DynamicJsonValue
                {
                    [nameof(ReplicationMessageReply.Type)] = ReplicationMessageReply.ReplyType.Error.ToString(),
                    [nameof(ReplicationMessageReply.MessageType)] = messageType,
                    [nameof(ReplicationMessageReply.LastEtagAccepted)] = -1,
                    [nameof(ReplicationMessageReply.Exception)] = e.ToString()
                };

                context.Write(writer, returnValue);
                writer.Flush();

                throw;
            }
        }

        protected void AddReplicationPerformance(IncomingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out stats);
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

        private void HandleReceivedDocumentsAndAttachmentsBatch(TOperationContext context, BlittableJsonReaderObject message, long lastDocumentEtag, IncomingReplicationStatsScope stats)
        {
            if (!message.TryGet(nameof(ReplicationMessageHeader.ItemsCount), out int itemsCount))
                throw new InvalidDataException($"Expected the '{nameof(ReplicationMessageHeader.ItemsCount)}' field, " +
                                               $"but had no numeric field of this value, this is likely a bug");

            if (!message.TryGet(nameof(ReplicationMessageHeader.AttachmentStreamsCount), out int attachmentStreamCount))
                throw new InvalidDataException($"Expected the '{nameof(ReplicationMessageHeader.AttachmentStreamsCount)}' field, " +
                                               $"but had no numeric field of this value, this is likely a bug");

            ReceiveSingleDocumentsBatch(context, itemsCount, attachmentStreamCount, lastDocumentEtag, stats);

            OnAttachmentStreamsReceives(attachmentStreamCount);

            OnDocumentsReceived();
        }

        protected void ReadItemsFromSource(int replicatedDocs, TOperationContext context, ByteStringContext allocator, IncomingReplicationHandler.DataForReplicationCommand data, Reader reader,
            IncomingReplicationStatsScope stats)
        {
            if (data.ReplicatedItems == null)
                data.ReplicatedItems = new ReplicationBatchItem[replicatedDocs];

            for (var i = 0; i < replicatedDocs; i++)
            {
                stats.RecordInputAttempt();

                var item = ReplicationBatchItem.ReadTypeAndInstantiate(reader);
                item.ReadChangeVectorAndMarker();
                item.Read(context, allocator, stats);

                data.ReplicatedItems[i] = item;
            }
        }

        protected void ReadAttachmentStreamsFromSource(int attachmentStreamCount,
            TOperationContext context, ByteStringContext allocator, IncomingReplicationHandler.DataForReplicationCommand dataForReplicationCommand, Reader reader, IncomingReplicationStatsScope stats)
        {
            if (attachmentStreamCount == 0)
                return;

            var replicatedAttachmentStreams = new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);

            for (var i = 0; i < attachmentStreamCount; i++)
            {
                var attachment = (AttachmentReplicationItem)ReplicationBatchItem.ReadTypeAndInstantiate(reader);
                Debug.Assert(attachment.Type == ReplicationBatchItem.ReplicationItemType.AttachmentStream);

                using (stats.For(ReplicationOperation.Incoming.AttachmentRead))
                {
                    attachment.ReadStream(allocator, _attachmentStreamsTempFile);
                    replicatedAttachmentStreams[attachment.Base64Hash] = attachment;
                }
            }

            dataForReplicationCommand.ReplicatedAttachmentStreams = replicatedAttachmentStreams;
        }

        protected abstract void EnsureNotDeleted(string nodeTag);

        protected abstract void OnDocumentsReceived();

        protected abstract void OnAttachmentStreamsReceives(int attachmentStreamCount);

        protected abstract void OnFailed(Exception exception);

        protected abstract void ReceiveSingleDocumentsBatch(TOperationContext context, int itemsCount, int attachmentStreamCount, long lastDocumentEtag, IncomingReplicationStatsScope stats);

        protected abstract int GetNextReplicationStatsId();

        internal abstract void SendHeartbeatStatusToSource(TOperationContext context, BlittableJsonTextWriter writer, long lastDocumentEtag, string notify);

        protected abstract void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null);

        protected abstract void HandleHeartbeatMessage(TOperationContext jsonOperationContext, BlittableJsonReaderObject blittableJsonReaderObject);

        public bool IsDisposed => _disposeOnce.Disposed;

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        protected virtual void DisposeInternal()
        {
            var releaser = _copiedBuffer.ReleaseBuffer;
            try
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Disposing IncomingReplicationHandler ({FromToString})");
                _cts.Cancel();

                try
                {
                    _connectionOptionsDisposable?.Dispose();
                }
                catch (Exception)
                {
                }
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
                    _tcpConnectionOptions.Dispose();
                }
                catch
                {
                    // do nothing
                }

                if (_incomingWork != PoolOfThreads.LongRunningWork.Current)
                {
                    try
                    {
                        _incomingWork?.Join(int.MaxValue);
                    }
                    catch (ThreadStateException)
                    {
                        // expected if the thread hasn't been started yet
                    }
                }
                _incomingWork = null;

                _cts.Dispose();

                _attachmentStreamsTempFile.Dispose();
                _replicationFromAnotherSource?.Dispose();
            }
            finally
            {
                try
                {
                    releaser?.Dispose();
                }
                catch (Exception)
                {
                    // can't do anything about it...
                }
            }
        }
    }
}
