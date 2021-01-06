using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationHandler : IDisposable, IReportOutgoingReplicationPerformance
    {
        public const string AlertTitle = "Replication";

        public event Action<OutgoingReplicationHandler> DocumentsSend;
        public event Action<LiveReplicationPulsesCollector.ReplicationPulse> HandleReplicationPulse;

        internal readonly DocumentDatabase _database;
        private readonly Logger _log;
        private readonly AsyncManualResetEvent _waitForChanges = new AsyncManualResetEvent();
        private readonly CancellationTokenSource _cts;
        private PoolOfThreads.LongRunningWork _longRunningSendingWork;
        internal readonly ReplicationLoader _parent;
        internal long _lastSentDocumentEtag;

        internal DateTime _lastDocumentSentTime;

        internal string LastAcceptedChangeVector;

        private TcpClient _tcpClient;

        private readonly AsyncManualResetEvent _connectionDisposed = new AsyncManualResetEvent();
        public bool IsConnectionDisposed => _connectionDisposed.IsSet;
        private JsonOperationContext.MemoryBuffer _buffer;

        internal CancellationToken CancellationToken => _cts.Token;

        internal string DestinationDbId;

        public long LastHeartbeatTicks;
        private Stream _stream;
        private InterruptibleRead _interruptibleRead;

        public event Action<OutgoingReplicationHandler, Exception> Failed;
        public event Action<OutgoingReplicationHandler> SuccessfulTwoWaysCommunication;
        public event Action<OutgoingReplicationHandler> SuccessfulReplication;

        public ReplicationNode Destination;
        private readonly bool _external;

        private readonly ConcurrentQueue<OutgoingReplicationStatsAggregator> _lastReplicationStats = new ConcurrentQueue<OutgoingReplicationStatsAggregator>();
        private OutgoingReplicationStatsAggregator _lastStats;
        private readonly TcpConnectionInfo _connectionInfo;

        private readonly TcpConnectionOptions _tcpConnectionOptions;
        // In case this is an outgoing pull replication from the hub
        // we need to associate this instance to the replication definition. 
        public string PullReplicationDefinitionName;

        public OutgoingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, bool external, TcpConnectionInfo connectionInfo)
        {
            _parent = parent;
            _database = database;
            Destination = node;
            _external = external;
            _log = LoggingSource.Instance.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _tcpConnectionOptions = tcpConnectionOptions ??
                                    new TcpConnectionOptions() {DocumentDatabase = database, Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,};
            _connectionInfo = connectionInfo;
            _database.Changes.OnDocumentChange += OnDocumentChange;
            _database.Changes.OnCounterChange += OnCounterChange;
            _database.Changes.OnTimeSeriesChange += OnTimeSeriesChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public OutgoingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            var lastStats = _lastStats;

            return _lastReplicationStats
                .Select(x => x == lastStats ? x.ToReplicationPerformanceLiveStatsWithDetails() : x.ToReplicationPerformanceStats())
                .ToArray();
        }

        public OutgoingReplicationStatsAggregator GetLatestReplicationPerformance()
        {
            return _lastStats;
        }

        public void Start()
        {
            _longRunningSendingWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleReplicationErrors(Replication), null, OutgoingReplicationThreadName);
        }

        public void StartPullReplicationAsHub(Stream stream, TcpConnectionHeaderMessage.SupportedFeatures supportedVersions)
        {
            SupportedFeatures = supportedVersions;
            _stream = stream;
            IsPullReplicationAsHub = true;
            OutgoingReplicationThreadName = $"Pull replication as hub {FromToString}";
            _longRunningSendingWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleReplicationErrors(PullReplication), null, OutgoingReplicationThreadName);
        }

        private string _outgoingReplicationThreadName;

        public string OutgoingReplicationThreadName
        {
            set => _outgoingReplicationThreadName = value;
            get => _outgoingReplicationThreadName ?? (_outgoingReplicationThreadName = $"Outgoing replication {FromToString}");
        }

        public bool IsPullReplicationAsHub;

        public string GetNode()
        {
            var node = Destination as InternalReplication;
            return node?.NodeTag;
        }

        private void PullReplication()
        {
            NativeMemory.EnsureRegistered();

            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
            if (_log.IsInfoEnabled)
                _log.Info($"Start pull replication as hub {FromToString}");

            using (_stream)
            using (_interruptibleRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream))
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out _buffer))
            {
                InitialHandshake();
                Replicate();
            }
        }

        private void Replication()
        {
            NativeMemory.EnsureRegistered();

            var certificate = _parent.GetCertificateForReplication(Destination, out var authorizationInfo);
            CertificateThumbprint = certificate?.Thumbprint;
            var database = _parent.Database;
            if (database == null)
                throw new InvalidOperationException("The database got disposed. Stopping the replication.");

            using (_parent._server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var rawRecord = _parent._server.Cluster.ReadRawDatabaseRecord(context, database.Name))
                {
                    if (rawRecord == null)
                        throw new InvalidOperationException($"The database record for {database.Name} does not exist?!");

                    if (rawRecord.IsEncrypted && Destination.Url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                        throw new InvalidOperationException(
                            $"{database.Name} is encrypted, and require HTTPS for replication, but had endpoint with url {Destination.Url} to database {Destination.Database}");
                }
            }

            var task = TcpUtils.ConnectSocketAsync(_connectionInfo, _parent._server.Engine.TcpConnectionTimeout, _log);
            task.Wait(CancellationToken);
            TcpClient tcpClient;
            string url;
            (tcpClient, url) = task.Result;
            using (Interlocked.Exchange(ref _tcpClient, tcpClient))
            {
                var wrapSsl = TcpUtils.WrapStreamWithSslAsync(_tcpClient, _connectionInfo, certificate, _parent._server.Server.CipherSuitesPolicy,
                    _parent._server.Engine.TcpConnectionTimeout);
                wrapSsl.Wait(CancellationToken);

                _stream = wrapSsl.Result;
                _interruptibleRead = new InterruptibleRead(_database.DocumentsStorage.ContextPool, _stream);

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.GetMemoryBuffer(out _buffer))
                {
                    var supportedFeatures = NegotiateReplicationVersion(authorizationInfo);
                    if (supportedFeatures.Replication.PullReplication)
                    {
                        SendPreliminaryData();
                        if (Destination is PullReplicationAsSink sink && (sink.Mode & PullReplicationMode.HubToSink) == PullReplicationMode.HubToSink)
                        {
                            if(supportedFeatures.Replication.PullReplication == false)
                                throw new InvalidOperationException("Other side does not support pull replication " + Destination);
                            InitiatePullReplicationAsSink(supportedFeatures, certificate);
                            return;
                        }
                    }

                    AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
                    if (_log.IsInfoEnabled)
                        _log.Info($"Will replicate to {Destination.FromString()} via {url}");

                    _tcpConnectionOptions.TcpClient = tcpClient;

                    using (_stream) // note that _stream is being disposed by the interruptible read
                    using (_interruptibleRead)
                    {
                        InitialHandshake();
                        _tcpConnectionOptions.DocumentDatabase.RunningTcpConnections.Add(_tcpConnectionOptions);
                        Replicate();
                    }
                }
            }
        }

        private void SendPreliminaryData()
        {
            var request = new DynamicJsonValue
            {
                ["Type"] = nameof(ReplicationInitialRequest),
            };

            if (Destination is PullReplicationAsSink destination)
            {
                request[nameof(ReplicationInitialRequest.Database)] = _parent.Database.Name; // my database
                request[nameof(ReplicationInitialRequest.DatabaseGroupId)] = _parent.Database.DatabaseGroupId; // my database id
                request[nameof(ReplicationInitialRequest.SourceUrl)] = _parent._server.GetNodeHttpServerUrl();
                request[nameof(ReplicationInitialRequest.Info)] = _parent._server.GetTcpInfoAndCertificates(null); // my connection info
                request[nameof(ReplicationInitialRequest.PullReplicationDefinitionName)] = destination.HubName;
                request[nameof(ReplicationInitialRequest.PullReplicationSinkTaskName)] = destination.GetTaskName();
            }

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, request);
                writer.Flush();
            }
        }

        private void InitiatePullReplicationAsSink(TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures, X509Certificate2 certificate)
        {
            var tcpOptions = new TcpConnectionOptions
            {
                ContextPool = _parent._server.Server._tcpContextPool,
                Stream = _stream,
                TcpClient = _tcpClient,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,
                DocumentDatabase = _database,
                ProtocolVersion = supportedFeatures.ProtocolVersion,
                Certificate = certificate
            };

            using (_parent._server.Server._tcpContextPool.AllocateOperationContext(out var ctx))
            using (ctx.GetMemoryBuffer(out _buffer))
            {
                _parent.RunPullReplicationAsSink(tcpOptions, _buffer, (PullReplicationAsSink)Destination);
            }
        }

        private void HandleReplicationErrors(Action replicationAction)
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
                if (_log.IsInfoEnabled)
                    _log.Info($"Operation canceled on replication thread ({FromToString}). " +
                              $"This is not necessary due to an issue. Stopped the thread.");
                if (_cts.IsCancellationRequested == false)
                {
                    Failed?.Invoke(this, e);
                }
            }

            void HandleIOException(IOException e)
            {
                if (_log.IsInfoEnabled)
                {
                    if (e.InnerException is SocketException)
                        _log.Info($"SocketException was thrown from the connection to remote node ({FromToString}). " +
                                  $"This might mean that the remote node is done or there is a network issue.", e);
                    else
                        _log.Info($"IOException was thrown from the connection to remote node ({FromToString}).", e);
                }
                Failed?.Invoke(this, e);
            }

            void HandleLegacyReplicationViolationException(LegacyReplicationViolationException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"LegacyReplicationViolationException occurred on replication thread ({FromToString}). " +
                              "Replication is stopped and will not continue until the violation is resolved. ", e);
                Failed?.Invoke(this, e);
            }

            void HandleException(Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Unexpected exception occurred on replication thread ({FromToString}). " +
                              $"Replication stopped (will be retried later).", e);
                Failed?.Invoke(this, e);
            }
        }

        public long NextReplicateTicks;

        private void Replicate()
        {
            using var documentSender = new ReplicationDocumentSender(_stream, this, _log, PathsToSend, _destinationAcceptablePaths);

            while (_cts.IsCancellationRequested == false)
            {
                while (_database.Time.GetUtcNow().Ticks > NextReplicateTicks)
                {
                    var once = _parent.DebugWaitAndRunReplicationOnce;
                    if (once != null)
                    {
                        once.Reset();
                        once.Wait(_cts.Token);
                    }

                    var sp = Stopwatch.StartNew();
                    var stats = _lastStats = new OutgoingReplicationStatsAggregator(_parent.GetNextReplicationStatsId(), _lastStats);
                    AddReplicationPerformance(stats);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingBegin);

                    try
                    {
                        using (var scope = stats.CreateScope())
                        {
                            try
                            {
                                if (Destination is InternalReplication dest)
                                {
                                    _parent.EnsureNotDeleted(dest.NodeTag);
                                }

                                var didWork = documentSender.ExecuteReplicationOnce(_tcpConnectionOptions, scope, ref NextReplicateTicks);
                                if (documentSender.MissingAttachmentsInLastBatch)
                                    continue;

                                if (didWork == false)
                                    break;

                                if (Destination is ExternalReplication externalReplication &&
                                    IsPullReplicationAsHub == false) // we might have a lot of pull connections and we don't want to keep track of them.
                                {
                                    var taskId = externalReplication.TaskId;
                                    UpdateExternalReplicationInfo(taskId);
                                }

                                DocumentsSend?.Invoke(this);

                                if (sp.ElapsedMilliseconds > 60 * 1000)
                                {
                                    _waitForChanges.Set();
                                    break;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // cancellation is not an actual error,
                                // it is a "notification" that we need to cancel current operation

                                const string msg = "Operation was canceled.";
                                AddReplicationPulse(ReplicationPulseDirection.OutgoingError, msg);

                                throw;
                            }
                            catch (Exception e)
                            {
                                AddReplicationPulse(ReplicationPulseDirection.OutgoingError, e.Message);

                                scope.AddError(e);
                                throw;
                            }
                        }
                    }
                    finally
                    {
                        stats.Complete();
                        AddReplicationPulse(ReplicationPulseDirection.OutgoingEnd);
                    }
                }

                OnSuccessfulReplication();

                //if this returns false, this means either timeout or canceled token is activated                    
                while (WaitForChanges(_parent.MinimalHeartbeatInterval, _cts.Token) == false)
                {
                    //If we got cancelled we need to break right away
                    if (_cts.IsCancellationRequested)
                        break;

                    // open tx
                    // read current change vector compare to last sent
                    // if okay, send cv
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (var tx = ctx.OpenReadTransaction())
                    {
                        var etag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        if (etag == _lastSentDocumentEtag)
                        {
                            SendHeartbeat(DocumentsStorage.GetDatabaseChangeVector(ctx));
                            _parent.CompleteDeletionIfNeeded(_cts);
                        }
                        else if (NextReplicateTicks > DateTime.UtcNow.Ticks)
                        {
                            SendHeartbeat(null);
                        }
                        else
                        {
                            //Send a heartbeat first so we will get an updated CV of the destination
                            var currentChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                            SendHeartbeat(null);
                            //If our previous CV is already merged to the destination wait a bit more 
                            if (ChangeVectorUtils.GetConflictStatus(LastAcceptedChangeVector, currentChangeVector) ==
                                ConflictStatus.AlreadyMerged)
                            {
                                continue;
                            }

                            // we have updates that we need to send to the other side
                            // let's do that.. 
                            // this can happen if we got replication from another node
                            // that we need to send to it. Note that we typically
                            // will wait for the other node to send the data directly to
                            // our destination, but if it doesn't, we'll step in.
                            // In this case, we try to limit congestion in the network and
                            // only send updates that we have gotten from someone else after
                            // a certain time, to let the other side tell us that it already
                            // got it. Note that this is merely an optimization to reduce network
                            // traffic. It is fine to have the same data come from different sources.
                            break;
                        }
                    }
                }

                _waitForChanges.Reset();
            }
        }

        private void InitialHandshake()
        {
            //start request/response for fetching last etag
            var request = new DynamicJsonValue
            {
                ["Type"] = "GetLastEtag",
                [nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = _database.DbId.ToString(),
                [nameof(ReplicationLatestEtagRequest.SourceDatabaseName)] = _database.Name,
                [nameof(ReplicationLatestEtagRequest.SourceUrl)] = _parent._server.GetNodeHttpServerUrl(),
                [nameof(ReplicationLatestEtagRequest.SourceTag)] = _parent._server.NodeTag,
                [nameof(ReplicationLatestEtagRequest.SourceMachineName)] = Environment.MachineName
            };
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, request);
                writer.Flush();
            }

            //handle initial response to last etag and staff
            try
            {
                var response = HandleServerResponse(getFullResponse: true);
                switch (response.ReplyType)
                {
                    //The first time we start replication we need to register the destination current CV
                    case ReplicationMessageReply.ReplyType.Ok:
                        LastAcceptedChangeVector = response.Reply.DatabaseChangeVector;
                        // this is used when the other side lets us know what paths it is going to accept from us
                        // it supplements (but does not extend) what we are willing to send out 
                        _destinationAcceptablePaths = response.Reply.AcceptablePaths;
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
                }
            }
            catch (DatabaseDoesNotExistException e)
            {
                var msg = $"Failed to parse initial server replication response, because there is no database named {_database.Name} " +
                          "on the other end. ";
                if (_external)
                    msg += "In order for the replication to work, a database with the same name needs to be created at the destination";

                var young = (DateTime.UtcNow - _startedAt).TotalSeconds < 30;
                if (young)
                    msg += "This can happen if the other node wasn't yet notified about being assigned this database and should be resolved shortly.";
                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);

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
                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);
                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                throw;
            }
            catch (Exception e)
            {
                var msg = $"{OutgoingReplicationThreadName} got an unexpected exception during initial handshake";
                if (_log.IsInfoEnabled)
                    _log.Info(msg, e);

                AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiateError, msg);
                AddAlertOnFailureToReachOtherSide(msg, e);

                throw;
            }
        }

        private void UpdateExternalReplicationInfo(long taskId)
        {
            var command = new UpdateExternalReplicationStateCommand(_database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = taskId,
                    NodeTag = _parent._server.NodeTag,
                    LastSentEtag = _lastSentDocumentEtag,
                    SourceChangeVector = _lastSentChangeVectorDuringHeartbeat,
                    DestinationChangeVector = LastAcceptedChangeVector
                }
            };

            // we don't wait to see if the command was applied on purpose
            _parent._server.SendToLeaderAsync(command)
                .IgnoreUnobservedExceptions();
        }

        private void AddReplicationPulse(ReplicationPulseDirection direction, string exceptionMessage = null)
        {
            HandleReplicationPulse?.Invoke(new LiveReplicationPulsesCollector.ReplicationPulse
            {
                OccurredAt = SystemTime.UtcNow,
                Direction = direction,
                To = Destination,
                IsExternal = _external,
                ExceptionMessage = exceptionMessage
            });
        }

        private void AddReplicationPerformance(OutgoingReplicationStatsAggregator stats)
        {
            _lastReplicationStats.Enqueue(stats);

            while (_lastReplicationStats.Count > 25)
                _lastReplicationStats.TryDequeue(out _);
        }

        private void AddAlertOnFailureToReachOtherSide(string msg, Exception e)
        {
            _database.NotificationCenter.Add(
                AlertRaised.Create(
                    _database.Name,
                    AlertTitle, msg, AlertType.Replication, NotificationSeverity.Warning, key: FromToString, details: new ExceptionDetails(e)));
        }

        private TcpConnectionHeaderMessage.SupportedFeatures NegotiateReplicationVersion(TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            {
                var parameters = new TcpNegotiateParameters
                {
                    Database = Destination.Database,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Replication,
                    SourceNodeTag = _parent._server.NodeTag,
                    DestinationNodeTag = GetNode(),
                    DestinationUrl = Destination.Url,
                    ReadResponseAndGetVersionCallback = ReadHeaderResponseAndThrowIfUnAuthorized,
                    Version = TcpConnectionHeaderMessage.ReplicationTcpVersion,
                    AuthorizeInfo = authorizationInfo
                };

                //This will either throw or return acceptable protocol version.
                SupportedFeatures = TcpNegotiation.NegotiateProtocolVersion(documentsContext, _stream, parameters);

                if (SupportedFeatures.ProtocolVersion <= 0)
                {
                    throw new InvalidOperationException(
                        $"{OutgoingReplicationThreadName}: TCP negotiation resulted with an invalid protocol version:{SupportedFeatures.ProtocolVersion}");
                }

                return SupportedFeatures;
            }
        }

        private int ReadHeaderResponseAndThrowIfUnAuthorized(JsonOperationContext context, BlittableJsonTextWriter writer, Stream stream, string url)
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
                        return headerResponse.Version;
                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new AuthorizationException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
                    case TcpConnectionStatus.TcpVersionMismatch:
                        if (headerResponse.Version != TcpNegotiation.OutOfRangeStatus)
                        {
                            return headerResponse.Version;
                        }
                        //Kindly request the server to drop the connection
                        SendDropMessage(context, writer, headerResponse);
                        throw new InvalidOperationException($"{Destination.FromString()} replied with failure {headerResponse.Message}");
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
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = _parent._server.NodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                [nameof(TcpConnectionHeaderMessage.Info)] =
                    $"Couldn't agree on replication TCP version ours:{TcpConnectionHeaderMessage.ReplicationTcpVersion} theirs:{headerResponse.Version}"
            });
            writer.Flush();
        }

        private bool WaitForChanges(int timeout, CancellationToken token)
        {
            while (true)
            {
                using (var result = _interruptibleRead.ParseToMemory(
                    _waitForChanges,
                    "replication notify message",
                    timeout,
                    _buffer,
                    token))
                {
                    if (result.Document != null)
                    {
                        HandleServerResponse(result.Document, allowNotify: true);
                    }
                    else
                    {
                        return result.Timeout == false;
                    }
                }
            }
        }

        internal void WriteToServer(DynamicJsonValue val)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
            {
                documentsContext.Write(writer, val);
            }
        }

        private void UpdateDestinationChangeVector(ReplicationMessageReply replicationBatchReply)
        {
            if (replicationBatchReply.MessageType == null)
                throw new InvalidOperationException(
                    "MessageType on replication response is null. This is likely is a symptom of an issue, and should be investigated.");

            LastAcceptedChangeVector = null;

            UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

        internal class UpdateSiblingCurrentEtag : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly ReplicationMessageReply _replicationBatchReply;
            private readonly AsyncManualResetEvent _trigger;
            private string _dbId;

            public UpdateSiblingCurrentEtag(ReplicationMessageReply replicationBatchReply, AsyncManualResetEvent trigger)
            {
                _replicationBatchReply = replicationBatchReply;
                _trigger = trigger;
            }

            public bool InitAndValidate(long lastReceivedEtag)
            {
                if (false == Init())
                {
                    return false;
                }

                return _replicationBatchReply.CurrentEtag >= lastReceivedEtag;
            }

            internal bool Init()
            {
                if (Guid.TryParse(_replicationBatchReply.DatabaseId, out Guid dbGuid) == false)
                    return false;

                _dbId = dbGuid.ToBase64Unpadded();

                return true;
            }

            internal bool DryRun(DocumentsOperationContext context)
            {
                var changeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                var status = ChangeVectorUtils.GetConflictStatus(_replicationBatchReply.DatabaseChangeVector,
                    changeVector);

                if (status != ConflictStatus.AlreadyMerged)
                    return false;

                var result = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, changeVector);
                return result.IsValid;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (string.IsNullOrEmpty(context.LastDatabaseChangeVector))
                    context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                var status = ChangeVectorUtils.GetConflictStatus(_replicationBatchReply.DatabaseChangeVector,
                    context.LastDatabaseChangeVector);

                if (status != ConflictStatus.AlreadyMerged)
                    return 0;

                var result = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, context.LastDatabaseChangeVector);
                if (result.IsValid)
                {
                    if (context.LastReplicationEtagFrom == null)
                        context.LastReplicationEtagFrom = new Dictionary<string, long>();

                    if (context.LastReplicationEtagFrom.ContainsKey(_replicationBatchReply.DatabaseId) == false)
                    {
                        context.LastReplicationEtagFrom[_replicationBatchReply.DatabaseId] = _replicationBatchReply.CurrentEtag;
                    }

                    context.LastDatabaseChangeVector = result.ChangeVector;

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
                }
               
                return result.IsValid ? 1 : 0;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new UpdateSiblingCurrentEtagDto
                {
                    ReplicationBatchReply = _replicationBatchReply
                };
            }
        }

        private void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            _lastSentDocumentEtag = replicationBatchReply.LastEtagAccepted;

            LastAcceptedChangeVector = replicationBatchReply.DatabaseChangeVector;
            if (_external == false)
            {
                var update = new UpdateSiblingCurrentEtag(replicationBatchReply, _waitForChanges);
                if (update.InitAndValidate(_lastDestinationEtag))
                {
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        if (update.DryRun(ctx))
                        {
                            // we intentionally not waiting here, there is nothing that depends on the timing on this, since this 
                            // is purely advisory. We just want to have the information up to date at some point, and we won't 
                            // miss anything much if this isn't there.
                            _database.TxMerger.Enqueue(update).IgnoreUnobservedExceptions();
                        }
                    }
                }
            }
            _lastDestinationEtag = replicationBatchReply.CurrentEtag;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (documentsContext.OpenReadTransaction())
            {
                if (DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction) !=
                    replicationBatchReply.LastEtagAccepted)
                {
                    // We have changes that the other side doesn't have, this can be because we have writes
                    // or because we have documents that were replicated to us. Either way, we need to sync
                    // those up with the remove side, so we'll start the replication loop again.
                    // We don't care if they are locally modified or not, because we filter documents that
                    // the other side already have (based on the change vector).
                    if ((DateTime.UtcNow - _lastDocumentSentTime).TotalMilliseconds > _parent.MinimalHeartbeatInterval)
                        _waitForChanges.Set();
                }
            }
        }

        private long _lastDestinationEtag;

        public string FromToString => $"from {_database.Name} at {_parent._server.NodeTag} to {Destination.FromString()}" +
                                      $"{(PullReplicationDefinitionName == null ? null : $"(pull definition: {PullReplicationDefinitionName})")}";

        public ReplicationNode Node => Destination;
        public string DestinationFormatted => $"{Destination.Url}/databases/{Destination.Database}";
        private string _lastSentChangeVectorDuringHeartbeat;
        internal void SendHeartbeat(string changeVector)
        {
            AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeat);

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var writer = new BlittableJsonTextWriter(documentsContext, _stream))
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
                        _lastSentChangeVectorDuringHeartbeat = changeVector;
                        heartbeat[nameof(ReplicationMessageHeader.DatabaseChangeVector)] = changeVector;
                    }
                    documentsContext.Write(writer, heartbeat);
                    writer.Flush();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Sending heartbeat failed. ({FromToString})", e);
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
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})");
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
                catch (Exception e)
                {
                    const string msg = "Parsing heartbeat result failed.";
                    if (_log.IsInfoEnabled)
                        _log.Info($"{msg} ({FromToString})", e);
                    AddReplicationPulse(ReplicationPulseDirection.OutgoingHeartbeatAcknowledgeError, msg);
                    throw;
                }
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

                    LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

                    var sendFullReply = replicationBatchReply.Type == ReplicationMessageReply.ReplyType.Error ||
                                        getFullResponse;

                    var type = replicationBatchReply.Type;
                    var reply = sendFullReply ? replicationBatchReply : null;
                    return (type, reply);
                }
            }
        }

        private static void ThrowTimeout(int timeout)
        {
            throw new TimeoutException("Could not get a server response in a reasonable time " +
                                       TimeSpan.FromMilliseconds(timeout));
        }

        private static void ThrowConnectionClosed()
        {
            throw new OperationCanceledException("The connection has been closed by the Dispose method");
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
                    break;
                case ReplicationMessageReply.ReplyType.MissingAttachments:
                    break;
            }

            switch (replicationBatchReply.Type)
            {
                case ReplicationMessageReply.ReplyType.Ok:
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

        private void OnDocumentChange(DocumentChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        private void OnCounterChange(CounterChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        private void OnTimeSeriesChange(TimeSeriesChange change)
        {
            OnChangeInternal(change.TriggeredByReplicationThread);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnChangeInternal(bool triggeredByReplicationThread)
        {
            if (triggeredByReplicationThread)
                return;
            _waitForChanges.Set();
        }

        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly DateTime _startedAt = DateTime.UtcNow;
        public string[] PathsToSend;
        private string[] _destinationAcceptablePaths;
        public string CertificateThumbprint;
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; private set; }

        public void Dispose()
        {
            // There are multiple invocations of dispose, this happens sometimes during tests, causing failures.
            if (!_disposed.Raise())
                return;

            var timeout = _parent._server.Engine.TcpConnectionTimeout;
            if (_log.IsInfoEnabled)
                _log.Info($"Disposing OutgoingReplicationHandler ({FromToString}) [Timeout:{timeout}]");

            _database.Changes.OnDocumentChange -= OnDocumentChange;
            _database.Changes.OnCounterChange -= OnCounterChange;
            _database.Changes.OnTimeSeriesChange -= OnTimeSeriesChange;

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

        private void OnSuccessfulTwoWaysCommunication() => SuccessfulTwoWaysCommunication?.Invoke(this);
        private void OnSuccessfulReplication() => SuccessfulReplication?.Invoke(this);
    }

    public interface IReportOutgoingReplicationPerformance
    {
        string DestinationFormatted { get; }
        OutgoingReplicationPerformanceStats[] GetReplicationPerformance();
    }

    public interface IReportIncomingReplicationPerformance
    {
        string DestinationFormatted { get; }
        IncomingReplicationPerformanceStats[] GetReplicationPerformance();
    }

    internal class UpdateSiblingCurrentEtagDto : TransactionOperationsMerger.IReplayableCommandDto<OutgoingReplicationHandler.UpdateSiblingCurrentEtag>
    {
        public ReplicationMessageReply ReplicationBatchReply;

        public OutgoingReplicationHandler.UpdateSiblingCurrentEtag ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new OutgoingReplicationHandler.UpdateSiblingCurrentEtag(ReplicationBatchReply, new AsyncManualResetEvent());
            command.Init();
            return command;
        }
    }

    public static class ReplicationMessageType
    {
        public const string Heartbeat = "Heartbeat";
        public const string Documents = "Documents";
    }
}
