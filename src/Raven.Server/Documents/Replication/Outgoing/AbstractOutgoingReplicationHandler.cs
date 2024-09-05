using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
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
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Tcp.Sync;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public abstract class AbstractOutgoingReplicationHandler<TContextPool, TOperationContext> : IAbstractOutgoingReplicationHandler
        where TContextPool : JsonContextPoolBase<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        private readonly TContextPool _contextPool;
        private readonly TcpConnectionInfo _connectionInfo;
        private readonly ServerStore _server;
        private readonly DateTime _startedAt = DateTime.UtcNow;
        private readonly string _databaseName;
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;
        private OutgoingReplicationStatsScope _statsInstance;
        private string _outgoingReplicationThreadName;
        internal readonly ReplicationDocumentSenderBase.ReplicationStats _stats = new ReplicationDocumentSenderBase.ReplicationStats();
        internal long _lastSentDocumentEtag;
        internal string DestinationDbId;

        protected readonly CancellationTokenSource _cts;
        protected PoolOfThreads.LongRunningWork _longRunningSendingWork;
        protected readonly AsyncManualResetEvent _connectionDisposed;
        protected JsonOperationContext.MemoryBuffer _buffer;
        protected Stream _stream;
        protected TcpClient _tcpClient;
        protected TcpConnectionOptions _tcpConnectionOptions;
        protected readonly ConcurrentQueue<OutgoingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<OutgoingReplicationStatsAggregator>();
        protected InterruptibleRead<TContextPool, TOperationContext> _interruptibleRead;
        protected OutgoingReplicationStatsAggregator _lastStats;
        protected RavenLogger Logger;

        public ServerStore Server => _server;
        public long LastSentDocumentEtag => _lastSentDocumentEtag;
        public TcpConnectionInfo ConnectionInfo => _connectionInfo;
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; protected set; }
        internal CancellationToken CancellationToken => _cts.Token;
        public bool IsConnectionDisposed => _connectionDisposed.IsSet;
        public ReplicationNode Destination { get; }
        public string LastSentChangeVector;
        public string LastAcceptedChangeVector { get; set; }
        public long LastHeartbeatTicks;
        public ReplicationNode Node => Destination;
        public string DestinationFormatted => $"{Destination.Url}/databases/{Destination.Database}";

        public int MissingAttachmentsRetries;

        public string OutgoingReplicationThreadName
        {
            set => _outgoingReplicationThreadName = value;
            get => _outgoingReplicationThreadName ?? (_outgoingReplicationThreadName = $"Outgoing replication {FromToString}");
        }

        public event Action<LiveReplicationPulsesCollector.ReplicationPulse> HandleReplicationPulse;

        public virtual string FromToString => $"from {_databaseName} at {_server.NodeTag} to {Destination.FromString()}";

        protected AbstractOutgoingReplicationHandler(TcpConnectionInfo connectionInfo, ServerStore server, string databaseName, AbstractDatabaseNotificationCenter notificationCenter, ReplicationNode node,
            TContextPool contextPool, CancellationToken token)
        {
            _connectionInfo = connectionInfo;
            _server = server;
            _databaseName = databaseName;
            _notificationCenter = notificationCenter;
            _contextPool = contextPool;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _connectionDisposed = new AsyncManualResetEvent(token);

            Logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), _databaseName);
            Destination = node;
        }

        public void Start()
        {
            _longRunningSendingWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleReplicationErrors(Replication), null, ThreadNames.ForOutgoingReplication(OutgoingReplicationThreadName,
                    _databaseName, Destination.FromString(), pullReplicationAsHub: false));
        }

        private void Replication()
        {
            NativeMemory.EnsureRegistered();

            var certificate = GetCertificateForReplication(Destination, out var authorizationInfo);

            AssertDatabaseNotDisposed();

            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(context, _databaseName))
                {
                    if (rawRecord == null)
                        throw new InvalidOperationException($"The database record for {_databaseName} does not exist?!");

                    if (rawRecord.IsEncrypted 
                        && Destination.Url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false 
                        && _server.Server.AllowEncryptedDatabasesOverHttp == false)
                        throw new InvalidOperationException(
                            $"{_databaseName} is encrypted, and require HTTPS for replication, but had endpoint with url {Destination.Url} to database {Destination.Database}");
                }
            }

            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out _buffer))
            {
                var task = TcpUtils.ConnectSecuredTcpSocketAsReplication(_connectionInfo, certificate, _server.Server.CipherSuitesPolicy,
                    (_, info, s, _, _) => NegotiateReplicationVersion(info, s, authorizationInfo),
                    _server.Engine.TcpConnectionTimeout, Logger, CancellationToken);
                task.Wait(CancellationToken);

                var socketResult = task.Result;

                _stream = socketResult.Stream;

                if (SupportedFeatures.ProtocolVersion <= 0)
                {
                    throw new InvalidOperationException(
                        $"{OutgoingReplicationThreadName}: TCP negotiation resulted with an invalid protocol version:{SupportedFeatures.ProtocolVersion}");
                }

                using (Interlocked.Exchange(ref _tcpClient, socketResult.TcpClient))
                {
                    if (socketResult.SupportedFeatures.DataCompression)
                    {
                        _stream = new ReadWriteCompressedStream(_stream, _buffer);
                        _tcpConnectionOptions.Stream = _stream;
                        _interruptibleRead = new InterruptibleRead<TContextPool, TOperationContext>(_contextPool, _stream);
                    }

                    if (socketResult.SupportedFeatures.Replication.PullReplication)
                    {
                        SendPreliminaryData();
                        if (Destination is PullReplicationAsSink sink && (sink.Mode & PullReplicationMode.HubToSink) == PullReplicationMode.HubToSink)
                        {
                            if (socketResult.SupportedFeatures.Replication.PullReplication == false)
                                throw new InvalidOperationException("Other side does not support pull replication " + Destination);
                            InitiatePullReplicationAsSink(socketResult.SupportedFeatures, certificate);
                            return;
                        }
                    }

                    AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Will replicate to {Destination.FromString()} via {socketResult.Url}");

                    _tcpConnectionOptions.TcpClient = socketResult.TcpClient;

                    using (_stream) // note that _stream is being disposed by the interruptible read
                    using (_interruptibleRead)
                    {
                        InitialHandshake();
                        _tcpConnectionOptions.DocumentDatabase?.RunningTcpConnections.Add(_tcpConnectionOptions);
                        _tcpConnectionOptions.DatabaseContext?.RunningTcpConnections.Add(_tcpConnectionOptions);
                        Replicate();
                    }
                }
            }
        }

        private Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateReplicationVersion(TcpConnectionInfo info, Stream stream, TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var parameters = new TcpNegotiateParameters
                {
                    Database = Destination.Database,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,
                    SourceNodeTag = _server.NodeTag,
                    DestinationNodeTag = GetNode(),
                    DestinationUrl = Destination.Url,
                    ReadResponseAndGetVersionCallback = ReadHeaderResponseAndThrowIfUnAuthorized,
                    Version = TcpConnectionHeaderMessage.ReplicationTcpVersion,
                    AuthorizeInfo = authorizationInfo,
                    DestinationServerId = info?.ServerId,
                    LicensedFeatures = new LicensedFeatures
                    {
                        DataCompression = _server.LicenseManager.LicenseStatus.HasTcpDataCompression &&
                                          _server.Configuration.Server.DisableTcpCompression == false

                    }
                };

                _interruptibleRead = new InterruptibleRead<TContextPool, TOperationContext>(_contextPool, stream);

                try
                {
                    //This will either throw or return acceptable protocol version.
                    SupportedFeatures = TcpNegotiation.Sync.NegotiateProtocolVersion(context, stream, parameters);
                    return Task.FromResult(SupportedFeatures);
                }
                catch
                {
                    _interruptibleRead.Dispose();
                    throw;
                }
            }
        }

        private TcpConnectionHeaderMessage.NegotiationResponse ReadHeaderResponseAndThrowIfUnAuthorized(JsonOperationContext context, BlittableJsonTextWriter writer, Stream stream, string url)
        {
            const int timeout = 2 * 60 * 1000;

            using (var replicationTcpConnectReplyMessage = _interruptibleRead.ParseToMemory(
                _connectionDisposed,
                "replication acknowledge response",
                timeout,
                _buffer,
                CancellationToken))
            {
                if (replicationTcpConnectReplyMessage.Timeout)
                {
                    ThrowTimeout(timeout);
                }
                if (replicationTcpConnectReplyMessage.Interrupted)
                {
                    ThrowConnectionClosed();
                }
                var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(replicationTcpConnectReplyMessage.Document);
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
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = _server.NodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                [nameof(TcpConnectionHeaderMessage.Info)] =
                    $"Couldn't agree on replication TCP version ours:{TcpConnectionHeaderMessage.ReplicationTcpVersion} theirs:{headerResponse.Version}"
            });
            writer.Flush();
        }

        protected virtual void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            switch (response.ReplyType)
            {
                //The first time we start replication we need to register the destination current CV
                case ReplicationMessageReply.ReplyType.Ok:
                    LastAcceptedChangeVector = response.Reply.DatabaseChangeVector;
                    break;

                case ReplicationMessageReply.ReplyType.Error:
                    var exception = new InvalidOperationException(response.Reply.Exception);
                    if (response.Reply.Exception.Contains(nameof(DatabaseDoesNotExistException)) ||
                        response.Reply.Exception.Contains(nameof(DatabaseNotRelevantException)))
                    {
                        AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, "Database does not exist");
                        DatabaseDoesNotExistException.ThrowWithMessageAndException(Destination.Database, response.Reply.Message, exception);
                    }

                    AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, $"Got error: {response.Reply.Exception}");
                    throw exception;

                default:
                    throw new ArgumentException($"Unknown handshake response type: '{response.ReplyType}'");
            }
        }

        internal (ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) HandleServerResponse(bool getFullResponse = false)
        {
            while (true)
            {
                var timeout = 2 * 60 * 1000;
                DebuggerAttachedTimeout.OutgoingReplication(ref timeout);

                using (var replicationBatchReplyMessage = _interruptibleRead.ParseToMemory(
                           _connectionDisposed,
                           "replication acknowledge message",
                           timeout,
                           _buffer,
                           CancellationToken))
                {
                    if (replicationBatchReplyMessage.Timeout)
                    {
                        ThrowTimeout(timeout);
                    }
                    if (replicationBatchReplyMessage.Interrupted)
                    {
                        ThrowConnectionClosed();
                    }

                    var replicationBatchReply = HandleServerResponse(replicationBatchReplyMessage.Document, allowNotify: false);
                    if (replicationBatchReply == null)
                        continue;

                    LastHeartbeatTicks = GetLastHeartbeatTicks();

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

            DestinationDbId = replicationBatchReply.DatabaseId;

            switch (replicationBatchReply.Type)
            {
                case ReplicationMessageReply.ReplyType.Ok:

                    UpdateDestinationChangeVector(replicationBatchReply);
                    OnSuccessfulTwoWaysCommunication();

                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug(
                            $"Received reply for replication batch from {Destination.FromString()}. New destination change vector is {LastAcceptedChangeVector}");
                    }
                    break;

                case ReplicationMessageReply.ReplyType.Error:
                    if (Logger.IsErrorEnabled)
                    {
                        Logger.Error(
                            $"Received reply for replication batch from {Destination.FromString()}. There has been a failure, error string received : {replicationBatchReply.Exception}");
                    }
                    throw new InvalidOperationException(
                        $"Received failure reply for replication batch. Error string received = {replicationBatchReply.Exception}");
                case ReplicationMessageReply.ReplyType.MissingAttachments:
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.Debug(
                            $"Received reply for replication batch from {Destination.FromString()}. Destination is reporting missing attachments.");
                    }

                    MissingAttachmentsRetries++;
                    if (MissingAttachmentsRetries > 1)
                    {
                        var msg = $"Failed to send batch successfully to {Destination.FromString()}. " +
                                  $"Destination reported missing attachments {MissingAttachmentsRetries} times.";
                        RaiseAlertAndThrowMissingAttachmentException(msg, replicationBatchReply.Exception);
                    }

                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(replicationBatchReply),
                        $"Received reply for replication batch with unrecognized type {replicationBatchReply.Type}" +
                        $"raw: {replicationBatchReplyMessage}");
            }

            return replicationBatchReply;
        }

        private void RaiseAlertAndThrowMissingAttachmentException(string msg, string exceptionDetails)
        {
            if (Logger.IsErrorEnabled)
            {
                Logger.Error(
                    $"Received reply for replication batch from {Destination.FromString()}. Error string received = {msg}");
            }

            _notificationCenter.Add(AlertRaised.Create(
                _databaseName,
                "Replication delay due to a missing attachments loop",
                msg + $"{Environment.NewLine}Please try to delete the missing attachment from '{_databaseName}' on node {_server.NodeTag} (see additional information regarding the document and attachment below)",
                AlertType.Replication,
                NotificationSeverity.Error,
                details: new ExceptionDetails { Exception = exceptionDetails }));

            throw new MissingAttachmentException($"{msg}.{Environment.NewLine}{exceptionDetails}");
        }

        protected virtual DynamicJsonValue GetInitialHandshakeRequest()
        {
            return new DynamicJsonValue
            {
                ["Type"] = "GetLastEtag",
                [nameof(ReplicationLatestEtagRequest.SourceDatabaseName)] = _databaseName,
                [nameof(ReplicationLatestEtagRequest.SourceUrl)] = _server.GetNodeHttpServerUrl(),
                [nameof(ReplicationLatestEtagRequest.SourceTag)] = _server.NodeTag,
                [nameof(ReplicationLatestEtagRequest.SourceMachineName)] = Environment.MachineName,
                [nameof(ReplicationLatestEtagRequest.ReplicationsType)] = GetReplicationType()
            };
        }

        private ReplicationLatestEtagRequest.ReplicationType GetReplicationType()
        {
            switch (this)
            {
                case OutgoingExternalReplicationHandler:
                case OutgoingPullReplicationHandler:
                    return ReplicationLatestEtagRequest.ReplicationType.External;
                case OutgoingInternalReplicationHandler:
                    return ReplicationLatestEtagRequest.ReplicationType.Internal;
                case OutgoingMigrationReplicationHandler:
                    return ReplicationLatestEtagRequest.ReplicationType.Migration;
                case ShardedOutgoingReplicationHandler:
                    return ReplicationLatestEtagRequest.ReplicationType.Sharded;
                default:
                    throw new ArgumentException($"Unknown type: {GetType()}");
            }
        }

        protected void InitialHandshake()
        {
            //start request/response for fetching last etag
            var request = GetInitialHandshakeRequest();

            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
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

                var young = (DateTime.UtcNow - _startedAt).TotalSeconds < 30;
                if (young)
                    msg += "This can happen if the other node wasn't yet notified about being assigned this database and should be resolved shortly.";
                if (Logger.IsInfoEnabled)
                    Logger.Info(msg, e);

                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);

                // won't add an alert on young connections
                // because it may take a few seconds for the other side to be notified by
                // the cluster that it has this db.
                if (young == false)
                    AddAlertOnFailureToReachOtherSide(msg, e);

                throw;
            }
            catch (OperationCanceledException e)
            {
                const string msg = "Got operation canceled notification while opening outgoing replication channel. " +
                                   "Aborting and closing the channel.";
                if (Logger.IsInfoEnabled)
                    Logger.Info(msg, e);
                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                throw;
            }
            catch (Exception e)
            {
                var msg = $"{OutgoingReplicationThreadName} got an unexpected exception during initial handshake";
                if (Logger.IsInfoEnabled)
                    Logger.Info(msg, e);

                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                AddAlertOnFailureToReachOtherSide(msg, e);

                throw;
            }
        }

        internal void SendHeartbeat(string changeVector)
        {
            AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeat);

            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
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
                        LastSentChangeVector = changeVector;
                        heartbeat[nameof(ReplicationMessageHeader.DatabaseChangeVector)] = changeVector;
                    }
                    context.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Sending heartbeat failed. ({FromToString})", e);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatError, "Sending heartbeat failed.");
                    throw;
                }

                try
                {
                    HandleServerResponse();
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledge);
                }
                catch (OperationCanceledException)
                {
                    const string msg = "Got cancellation notification while parsing heartbeat response. Closing replication channel.";
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"{msg} ({FromToString})");
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
                catch (Exception e)
                {
                    const string msg = "Parsing heartbeat result failed.";
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"{msg} ({FromToString})", e);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
            }
        }

        public string GetNode()
        {
            var node = Destination as InternalReplication;
            return node?.NodeTag;
        }

        public OutgoingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            var lastStats = _lastStats;

            return _lastReplicationStats
                .Select(x => x == lastStats ? x.ToReplicationPerformanceLiveStatsWithDetails() : x.ToReplicationPerformanceStats())
                .ToArray();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureValidStats(OutgoingReplicationStatsScope stats)
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

        internal void WriteToServer(DynamicJsonValue val)
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, val);
            }
        }

        protected void AddReplicationPerformance(OutgoingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out _);
        }

        protected void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
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

        [DoesNotReturn]
        protected static void ThrowTimeout(int timeout)
        {
            throw new TimeoutException("Could not get a server response in a reasonable time " +
                                       TimeSpan.FromMilliseconds(timeout));
        }

        [DoesNotReturn]
        protected static void ThrowConnectionClosed()
        {
            throw new OperationCanceledException("The connection has been closed by the Dispose method");
        }

        protected virtual void HandleReplicationErrors(Action replicationAction)
        {
            try
            {
                replicationAction();
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Count == 1)
                {
                    if (e.InnerException is OperationCanceledException oce)
                    {
                        HandleOperationCancelException(oce);
                    }

                    if (e.InnerException is IOException ioe)
                    {
                        HandleIOException(ioe);
                    }
                }

                HandleException(e);
            }
            catch (MissingAttachmentException e)
            {
                HandleException(e);
            }
            catch (OperationCanceledException e)
            {
                HandleOperationCancelException(e);
            }
            catch (IOException e)
            {
                HandleIOException(e);
            }
            catch (LegacyReplicationViolationException e)
            {
                HandleLegacyReplicationViolationException(e);
            }
            catch (Exception e)
            {
                HandleException(e);
            }

            void HandleOperationCancelException(OperationCanceledException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Operation canceled on replication thread ({FromToString}). " +
                                $"This is not necessarily due to an issue. Stopped the thread.");
                if (_cts.IsCancellationRequested == false)
                {
                    OnFailed(e);
                }
            }

            void HandleIOException(IOException e)
            {
                if (Logger.IsInfoEnabled)
                {
                    if (e.InnerException is SocketException)
                        Logger.Info($"SocketException was thrown from the connection to remote node ({FromToString}). " +
                                    $"This might mean that the remote node is done or there is a network issue.", e);
                    else
                        Logger.Info($"IOException was thrown from the connection to remote node ({FromToString}).", e);
                }
                OnFailed(e);
            }

            void HandleLegacyReplicationViolationException(LegacyReplicationViolationException e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"LegacyReplicationViolationException occurred on replication thread ({FromToString}). " +
                                "Replication is stopped and will not continue until the violation is resolved. ", e);
                OnFailed(e);
            }

            void HandleException(Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Unexpected exception occurred on replication thread ({FromToString}). " +
                                $"Replication stopped (will be retried later).", e);
                OnFailed(e);
            }
        }

        protected virtual DynamicJsonValue GetSendPreliminaryDataRequest()
        {
            return new DynamicJsonValue
            {
                ["Type"] = nameof(ReplicationInitialRequest)
            };
        }
        private void SendPreliminaryData()
        {
            var request = GetSendPreliminaryDataRequest();

            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, request);
                writer.Flush();
            }
        }

        protected void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null)
        {
            HandleReplicationPulse?.Invoke(new LiveReplicationPulsesCollector.ReplicationPulse
            {
                OccurredAt = SystemTime.UtcNow,
                Direction = direction,
                To = Destination,
                IsExternal = GetType() != typeof(OutgoingInternalReplicationHandler),
                ExceptionMessage = exceptionMessage
            });
        }

        protected abstract void AddAlertOnFailureToReachOtherSide(string msg, Exception e);

        protected abstract void Replicate();

        protected abstract void OnSuccessfulTwoWaysCommunication();

        protected abstract void OnFailed(Exception e);

        protected abstract long GetLastHeartbeatTicks();

        protected abstract void InitiatePullReplicationAsSink(TcpConnectionHeaderMessage.SupportedFeatures socketResultSupportedFeatures, X509Certificate2 certificate);

        protected abstract void AssertDatabaseNotDisposed();

        protected abstract X509Certificate2 GetCertificateForReplication(ReplicationNode destination, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo);

        protected abstract void OnBeforeDispose();


        private readonly SingleUseFlag _disposed = new SingleUseFlag();

        public virtual void Dispose()
        {
            // There are multiple invocations of dispose, this happens sometimes during tests, causing failures.
            if (!_disposed.Raise())
                return;

            var timeout = _server.Engine.TcpConnectionTimeout;
            if (Logger.IsInfoEnabled)
                Logger.Info($"Disposing {GetType().FullName} ({FromToString}) [Timeout:{timeout}]");

            OnBeforeDispose();

            _cts.Cancel();

            _tcpConnectionOptions.Dispose();
            DisposeTcpClient();

            _connectionDisposed.Set();

            if (_longRunningSendingWork != null && _longRunningSendingWork != PoolOfThreads.LongRunningWork.Current)
            {
                while (_longRunningSendingWork.Join((int)timeout.TotalMilliseconds) == false)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Waited {timeout} for timeout to occur, but still this thread is keep on running. Will wait another {timeout} ");
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
