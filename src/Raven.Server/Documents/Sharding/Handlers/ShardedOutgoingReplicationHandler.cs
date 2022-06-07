extern alias NGC;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Tcp.Sync;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Json.Sync;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOutgoingReplicationHandler : IDisposable
    {
        private readonly int _shard;
        private readonly ShardedDatabaseContext.ShardedReplicationContext _parent;
        private PoolOfThreads.LongRunningWork _longRunningSendingWork;
        private readonly string _databaseName;
        private JsonOperationContext.MemoryBuffer _buffer;
        private readonly TcpConnectionOptions _tcpConnectionOptions;
        private readonly TcpConnectionInfo _connectionInfo;
        private readonly ReplicationQueue _replicationQueue;
        private readonly AsyncManualResetEvent _connectionDisposed;
        private Stream _stream;
        private readonly Logger _log;
        private readonly CancellationTokenSource _cts;
        private TcpClient _tcpClient;
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly ReplicationDocumentSenderBase.ReplicationStats _stats = new ReplicationDocumentSenderBase.ReplicationStats();
        private OutgoingReplicationStatsAggregator _lastStats;
        internal CancellationToken CancellationToken => _cts.Token;
        public bool IsConnectionDisposed => _connectionDisposed.IsSet;
        public readonly ReplicationNode Destination;
        private JsonOperationContext _context;
        private long _lastSentDocumentEtag;
        private long _lastEtag;
        private OutgoingReplicationStatsScope _statsInstance;

        public TcpConnectionInfo ConnectionInfo => _connectionInfo;
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; private set; }

        public long LastHeartbeatTicks;


        public ShardedOutgoingReplicationHandler(ShardedDatabaseContext.ShardedReplicationContext parent, ShardReplicationNode node, int shardNumber,
            TcpConnectionInfo connectionInfo, ReplicationQueue replicationQueue)
        {
            _parent = parent;
            _shard = shardNumber;
            _databaseName = node.Database;
            _log = LoggingSource.Instance.GetLogger(_databaseName, GetType().FullName);
            _tcpConnectionOptions = new TcpConnectionOptions
            {
                DatabaseContext = parent.Context,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication
            };
            _connectionInfo = connectionInfo;
            _replicationQueue = replicationQueue;

            _connectionDisposed = new AsyncManualResetEvent(parent.Context.DatabaseShutdown);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(parent.Context.DatabaseShutdown);

            Destination = node;
        }

        public void Start()
        {
            _longRunningSendingWork =
                    PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => Replication(), null, "ShardedOutgoingReplicationThreadName");
        }

        private void Replication()
        {
            NativeMemory.EnsureRegistered();

            var certificate = _parent.GetCertificateForReplication(Destination, out var authorizationInfo);

            using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var rawRecord = _parent.Server.Cluster.ReadRawDatabaseRecord(context, _databaseName))
                {
                    if (rawRecord == null)
                        throw new InvalidOperationException($"The database record for {_databaseName} does not exist?!");

                    if (rawRecord.IsEncrypted && Destination.Url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                        throw new InvalidOperationException(
                            $"{_databaseName} is encrypted, and require HTTPS for replication, but had endpoint with url {Destination.Url} to database {Destination.Database}");
                }
            }

            using (_parent.Context.AllocateContext(out _context))
            using (_context.GetMemoryBuffer(out _buffer))
            {
                var task = TcpUtils.ConnectSecuredTcpSocketAsReplication(_connectionInfo, certificate, _parent.Server.Server.CipherSuitesPolicy,
                    (_, info, s, _, _) => NegotiateReplicationVersion(info, s, authorizationInfo),
                    _parent.Server.Engine.TcpConnectionTimeout, _log, CancellationToken);
                task.Wait(CancellationToken);

                var socketResult = task.Result;

                _stream = socketResult.Stream;

                if (SupportedFeatures.ProtocolVersion <= 0)
                {
                    throw new InvalidOperationException(
                        $"TCP negotiation resulted with an invalid protocol version:{SupportedFeatures.ProtocolVersion}");
                }

                using (Interlocked.Exchange(ref _tcpClient, socketResult.TcpClient))
                {
                    if (socketResult.SupportedFeatures.DataCompression)
                    {
                        _stream = new ReadWriteCompressedStream(_stream, _buffer);
                        _tcpConnectionOptions.Stream = _stream;
                    }

                    if (socketResult.SupportedFeatures.Replication.PullReplication)
                    {
                        SendPreliminaryData();
                    }

                    if (_log.IsInfoEnabled)
                        _log.Info($"Will replicate to {Destination.FromString()} via {socketResult.Url}");

                    _tcpConnectionOptions.TcpClient = socketResult.TcpClient;

                    using (_stream)
                    {
                        InitialHandshake();
                        Replicate();
                    }
                }
            }
        }

        private void Replicate()
        {
            while (_cts.IsCancellationRequested == false)
            {
                while (_replicationQueue.Items[_shard].TryTake(out var items))
                {
                    using (_parent.Server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);

                        using (var scope = stats.CreateScope())
                        {
                            EnsureValidStats(scope);

                            if (items.Count == 0)
                            {
                                SendHeartbeat(null);
                                _replicationQueue.SendToShardCompletion.Signal();
                                continue;
                            }

                            using (_stats.Network.Start())
                            {
                                MissingAttachmentsInLastBatch = false;

                                var didWork = SendDocumentsBatch(context, items, _stats.Network);

                                if (MissingAttachmentsInLastBatch) // TODO
                                    continue;

                                _replicationQueue.SendToShardCompletion.Signal();

                                if (didWork == false)
                                    break;
                            }
                        }
                    }
                }
            }
        }

        private bool SendDocumentsBatch(TransactionOperationContext context, List<ReplicationBatchItem> items, OutgoingReplicationStatsScope stats)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                var headerJson = new DynamicJsonValue
                {
                    [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                    [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                    [nameof(ReplicationMessageHeader.ItemsCount)] = items.Count,
                    [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = _replicationQueue.AttachmentsPerShard[_shard].Count
                };

                WriteToServer(headerJson);

                foreach (var item in items)
                {
                    using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
                    {
                        item.Write(cv, _stream, _tempBuffer, stats);
                    }
                }

                foreach (var kvp in _replicationQueue.AttachmentsPerShard[_shard])
                {
                    using (stats.For(ReplicationOperation.Outgoing.AttachmentRead))
                    {
                        using (var attachment = kvp.Value)
                        {
                            try
                            {
                                attachment.WriteStream(_stream, _tempBuffer);
                                stats.RecordAttachmentOutput(attachment.Stream.Length);
                            }
                            catch
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Failed to write Attachment stream {FromToString}");

                                throw;
                            }
                        }
                    }
                }

                _stream.Flush();
                sw.Stop();

                var (type, _) = HandleServerResponse();

                if (type == ReplicationMessageReply.ReplyType.MissingAttachments)
                {
                    MissingAttachmentsInLastBatch = true;
                    return false;
                }

                _lastSentDocumentEtag = _lastEtag;
            }
            finally
            {
                foreach (var item in items)
                {
                    item.Dispose();
                }

          
                _replicationQueue.AttachmentsPerShard[_shard].Clear();
            }

            return true;
        }

        public bool MissingAttachmentsInLastBatch { get; set; }

        internal void WriteToServer(DynamicJsonValue val)
        {
            using (_parent.Context.AllocateContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, val);
            }
        }

        private void SendPreliminaryData()
        {
            var request = new DynamicJsonValue
            {
                ["Type"] = nameof(ReplicationInitialRequest)
            };

            if (Destination is PullReplicationAsSink destination)
            {
                request[nameof(ReplicationInitialRequest.Database)] = _databaseName; // my database
                request[nameof(ReplicationInitialRequest.SourceUrl)] = _parent.Server.GetNodeHttpServerUrl();
                request[nameof(ReplicationInitialRequest.Info)] = _parent.Server.GetTcpInfoAndCertificates(null); // my connection info
                request[nameof(ReplicationInitialRequest.PullReplicationDefinitionName)] = destination.HubName;
                request[nameof(ReplicationInitialRequest.PullReplicationSinkTaskName)] = destination.GetTaskName();
            }

            using (_parent.Context.AllocateContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, request);
                writer.Flush();
            }
        }

        private Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateReplicationVersion(TcpConnectionInfo info, Stream stream, TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            using (_parent.Context.AllocateContext(out JsonOperationContext context))
            {
                var parameters = new TcpNegotiateParameters
                {
                    Database = Destination.Database,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,
                    SourceNodeTag = _parent.Server.NodeTag,
                    DestinationNodeTag = GetNode(),
                    DestinationUrl = Destination.Url,
                    ReadResponseAndGetVersionCallback = ReadHeaderResponseAndThrowIfUnAuthorized,
                    Version = TcpConnectionHeaderMessage.ReplicationTcpVersion,
                    AuthorizeInfo = authorizationInfo,
                    DestinationServerId = info?.ServerId,
                    LicensedFeatures = new LicensedFeatures
                    {
                        DataCompression = _parent.Server.LicenseManager.LicenseStatus.HasTcpDataCompression &&
                                          _parent.Server.Configuration.Server.DisableTcpCompression == false
                    }
                };

                try
                {
                    //This will either throw or return acceptable protocol version.
                    SupportedFeatures = TcpNegotiation.Sync.NegotiateProtocolVersion(context, stream, parameters);
                    return Task.FromResult(SupportedFeatures);
                }
                catch
                {
                    throw;
                }
            }
        }

        private void InitialHandshake()
        {
            //start request/response for fetching last etag
            var request = new DynamicJsonValue
            {
                ["Type"] = "GetLastEtag",
                [nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _parent.SourceDatabaseId,
                [nameof(ReplicationLatestEtagRequest.SourceDatabaseName)] = _databaseName,
                [nameof(ReplicationLatestEtagRequest.SourceUrl)] = _parent.Server.GetNodeHttpServerUrl(),
                [nameof(ReplicationLatestEtagRequest.SourceTag)] = _parent.Server.NodeTag,
                [nameof(ReplicationLatestEtagRequest.SourceMachineName)] = Environment.MachineName,
                [nameof(ReplicationLatestEtagRequest.ReplicationsType)] = ReplicationLatestEtagRequest.ReplicationType.Sharded
            };
            using (_parent.Context.AllocateContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, request);
                writer.Flush();
            }

            //handle initial response to last etag and staff
            try
            {
                var response = HandleServerResponse(getFullResponse: true);
                ProcessHandshakeResponse(response);
            }
            catch (DatabaseDoesNotExistException e)
            {
                var msg = $"Failed to parse initial server replication response, because there is no database named {_databaseName} " +
                          "on the other end. ";

                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);
                throw;
            }
            catch (OperationCanceledException e)
            {
                const string msg = "Got operation canceled notification while opening outgoing replication channel. " +
                                   "Aborting and closing the channel.";
                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);

                throw;
            }
            catch (Exception e)
            {
                var msg = $"ShardedOutgoingReplicationThreadName got an unexpected exception during initial handshake";
                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);

                throw;
            }
        }

        private void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            switch (response.ReplyType)
            {
                case ReplicationMessageReply.ReplyType.Ok:
                    LastAcceptedChangeVector = response.Reply.DatabaseChangeVector;
                    break;

                case ReplicationMessageReply.ReplyType.Error:
                    var exception = new InvalidOperationException(response.Reply.Exception);
                    if (response.Reply.Exception.Contains(nameof(DatabaseDoesNotExistException)) ||
                        response.Reply.Exception.Contains(nameof(DatabaseNotRelevantException)))
                    {
                        DatabaseDoesNotExistException.ThrowWithMessageAndException(Destination.Database, response.Reply.Message, exception);
                    }

                    throw exception;

                default:
                    throw new ArgumentException($"Unknown handshake response type: '{response.ReplyType}'");
            }
        }

        public string LastAcceptedChangeVector { get; set; }

        internal (ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) HandleServerResponse(bool getFullResponse = false)
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000;
                DebuggerAttachedTimeout.OutgoingReplication(ref timeout);

                using (var replicationBatchReplyMessage = _context.Sync.ParseToMemory(
                           _stream,
                           "replication acknowledge response",
                           BlittableJsonDocumentBuilder.UsageMode.None,
                           _buffer))
                {

                    if (replicationBatchReplyMessage == null)
                        continue;

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage, allowNotify: false);

                    if (replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = _parent.Context.Time.GetUtcNow().Ticks;

                    var sendFullReply = replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error ||
                                        getFullResponse;

                    var type = replicationBatchReply.Type;
                    var reply = sendFullReply ? replicationBatchReply : null;
                    return (type, reply);
                }
            }
        }

        internal ReplicationMessageReply HandleServerResponse(BlittableJsonReaderObject replicationBatchReplyMessage, bool allowNotify)
        {
            replicationBatchReplyMessage.BlittableValidation();
            var replicationBatchReply = JsonDeserializationServer.ReplicationMessageReply(replicationBatchReplyMessage);

            if (replicationBatchReply.MessageType == "Processing")
                return null;

            if (allowNotify == false && replicationBatchReply.MessageType == "Notify")
                return null;

            switch (replicationBatchReply.Type)
            {
                case ReplicationMessageReply.ReplyType.Ok:

                    UpdateDestinationChangeVector(replicationBatchReply);

                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Received reply for replication batch from {Destination.FromString()}. New destination change vector is {LastAcceptedChangeVector}");
                    }
                    break;

                case ReplicationMessageReply.ReplyType.Error:
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Received reply for replication batch from {Destination.FromString()}. There has been a failure, error string received : {replicationBatchReply.Exception}");
                    }
                    throw new InvalidOperationException(
                        $"Received failure reply for replication batch. Error string received = {replicationBatchReply.Exception}");
                case ReplicationMessageReply.ReplyType.MissingAttachments:
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Received reply for replication batch from {Destination.FromString()}. Destination is reporting missing attachments.");
                    }
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(replicationBatchReply),
                        $"Received reply for replication batch with unrecognized type {replicationBatchReply.Type}" +
                        $"raw: {replicationBatchReplyMessage}");
            }

            return replicationBatchReply;
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            if (replicationBatchReply.MessageType == null)
                throw new InvalidOperationException(
                    "MessageType on replication response is null. This is likely is a symptom of an issue, and should be investigated.");

            LastAcceptedChangeVector = null;

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        protected virtual void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = replicationBatchReply.LastEtagAccepted;

            LastAcceptedChangeVector = replicationBatchReply.DatabaseChangeVector;
        }


        public string GetNode()
        {
            var node = Destination as InternalReplication;
            return node?.NodeTag;
        }

        private TcpConnectionHeaderMessage.NegotiationResponse ReadHeaderResponseAndThrowIfUnAuthorized(JsonOperationContext context, BlittableJsonTextWriter writer, Stream stream, string url)
        {
            using (var replicationTcpConnectReplyMessage = context.Sync.ParseToMemory(
                       stream,
                       "replication acknowledge response",
                       BlittableJsonDocumentBuilder.UsageMode.None,
                       _buffer))
            {
                var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(replicationTcpConnectReplyMessage);
                switch (headerResponse.Status)
                {
                    case TcpConnectionStatus.Ok:
                        return new TcpConnectionHeaderMessage.NegotiationResponse
                        {
                            Version = headerResponse.Version,
                            LicensedFeatures = headerResponse.LicensedFeatures
                        };

                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new AuthorizationException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                    case TcpConnectionStatus.TcpVersionMismatch:
                        if (headerResponse.Version != TcpNegotiation.OutOfRangeStatus)
                        {
                            return new TcpConnectionHeaderMessage.NegotiationResponse
                            {
                                Version = headerResponse.Version,
                                LicensedFeatures = headerResponse.LicensedFeatures
                            };
                        }

                        //Kindly request the server to drop the connection
                        SendDropMessage(context, writer, headerResponse);
                        throw new InvalidOperationException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                    case TcpConnectionStatus.InvalidNetworkTopology:
                        throw new InvalidNetworkTopologyException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                    default:
                        throw new InvalidOperationException($"{Destination.FromString()} replied with unknown status {headerResponse.Status}, message:{headerResponse.Message}");
                }
            }
        }

        private void SendDropMessage(JsonOperationContext context, BlittableJsonTextWriter writer, TcpConnectionHeaderResponse headerResponse)
        {
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = Destination.Database,
                [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop.ToString(),
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = _parent.Server.NodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                [nameof(TcpConnectionHeaderMessage.Info)] =
                    $"Couldn't agree on replication TCP version ours:{TcpConnectionHeaderMessage.ReplicationTcpVersion} theirs:{headerResponse.Version}"
            });
            writer.Flush();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(OutgoingReplicationStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;
            _stats.Storage = stats.For(ReplicationOperation.Outgoing.Storage, start: false);
            _stats.Network = stats.For(ReplicationOperation.Outgoing.Network, start: false);

            _stats.DocumentRead = _stats.Storage.For(ReplicationOperation.Outgoing.DocumentRead, start: false);
            _stats.TombstoneRead = _stats.Storage.For(ReplicationOperation.Outgoing.TombstoneRead, start: false);
            _stats.AttachmentRead = _stats.Storage.For(ReplicationOperation.Outgoing.AttachmentRead, start: false);
            _stats.CounterRead = _stats.Storage.For(ReplicationOperation.Outgoing.CounterRead, start: false);
            _stats.TimeSeriesRead = _stats.Storage.For(ReplicationOperation.Outgoing.TimeSeriesRead, start: false);
        }

        private string FromToString => $"from {_databaseName} at {_parent.Server.NodeTag} to {Destination.FromString()}";

        internal void SendHeartbeat(string changeVector)
        {
            using (_parent.Context.AllocateContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                try
                {
                    var heartbeat = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastSentDocumentEtag,
                        [nameof(ReplicationMessageHeader.ItemsCount)] = 0
                    };
                    if (changeVector != null)
                    {
                        LastSentChangeVectorDuringHeartbeat = changeVector;
                        heartbeat[nameof(ReplicationMessageHeader.DatabaseChangeVector)] = changeVector;
                    }
                    context.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Sending heartbeat failed. ({FromToString})", e);
                    throw;
                }

                try
                {
                    HandleServerResponse();
                }
                catch (OperationCanceledException)
                {
                    const string msg = "Got cancellation notification while parsing heartbeat response. Closing replication channel.";
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})");
                    throw;
                }
                catch (Exception e)
                {
                    const string msg = "Parsing heartbeat result failed.";
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})", e);
                    throw;
                }
            }
        }

        public string LastSentChangeVectorDuringHeartbeat { get; set; }

        private readonly SingleUseFlag _disposed = new SingleUseFlag();

        public void Dispose()
        {
            // There are multiple invocations of dispose, this happens sometimes during tests, causing failures.
            if (!_disposed.Raise())
                return;

            var timeout = _parent.Server.Engine.TcpConnectionTimeout;
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString}) [Timeout:{timeout}]");

            _cts.Cancel();

            _tcpConnectionOptions.Dispose();
            DisposeTcpClient();

            _connectionDisposed.Set();

            if (_longRunningSendingWork != null && _longRunningSendingWork != PoolOfThreads.LongRunningWork.Current)
            {
                while (_longRunningSendingWork.Join((int)timeout.TotalMilliseconds) == false)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Waited {timeout} for timeout to occur, but still this thread is keep on running. Will wait another {timeout} ");
                    DisposeTcpClient();
                }
            }

            try
            {
                _cts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //was already disposed? we don't care, we are disposing
            }

            _connectionDisposed.Dispose();
        }

        private void DisposeTcpClient()
        {
            try
            {
                Volatile.Read(ref _tcpClient)?.Dispose();
            }
            catch (Exception)
            {
                // nothing we can do here
            }
        }
    }
}

