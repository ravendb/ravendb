using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationLoader : IDisposable, ITombstoneAware
    {
        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public event Action<IncomingReplicationHandler> IncomingReplicationAdded;

        public event Action<IncomingReplicationHandler> IncomingReplicationRemoved;

        public event Action<OutgoingReplicationHandler> OutgoingReplicationAdded;

        public event Action<OutgoingReplicationHandler> OutgoingReplicationRemoved;

        internal ManualResetEventSlim DebugWaitAndRunReplicationOnce;

        public DocumentDatabase Database;
        private SingleUseFlag _isInitialized = new SingleUseFlag();

        private readonly Timer _reconnectAttemptTimer;
        internal readonly int MinimalHeartbeatInterval;

        public ResolveConflictOnReplicationConfigurationChange ConflictResolver;

        private readonly ConcurrentSet<OutgoingReplicationHandler> _outgoing =
            new ConcurrentSet<OutgoingReplicationHandler>();

        private readonly ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo> _outgoingFailureInfo =
            new ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo>();

        private readonly ConcurrentDictionary<string, IncomingReplicationHandler> _incoming =
            new ConcurrentDictionary<string, IncomingReplicationHandler>();

        private readonly ConcurrentDictionary<IncomingConnectionInfo, DateTime> _incomingLastActivityTime =
            new ConcurrentDictionary<IncomingConnectionInfo, DateTime>();

        private readonly ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>
            _incomingRejectionStats =
                new ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>();

        private readonly ConcurrentSet<ConnectionShutdownInfo> _reconnectQueue =
            new ConcurrentSet<ConnectionShutdownInfo>();

        private readonly ConcurrentBag<ReplicationNode> _internalDestinations = new ConcurrentBag<ReplicationNode>();
        private readonly HashSet<ExternalReplicationBase> _externalDestinations = new HashSet<ExternalReplicationBase>();

        private HubInfoForCleaner _hubInfoForCleaner;

        private class HubInfoForCleaner
        {
            public long LastEtag;
            public DateTime LastCleanup;
        }

        private class LastEtagPerDestination
        {
            public long LastEtag;
        }

        private int _replicationStatsId;

        private readonly ConcurrentDictionary<ReplicationNode, LastEtagPerDestination> _lastSendEtagPerDestination =
            new ConcurrentDictionary<ReplicationNode, LastEtagPerDestination>();

        public long GetMinimalEtagForReplication()
        {
            var replicationNodes = Destinations?.ToList();
            if (replicationNodes == null || replicationNodes.Count == 0)
                return long.MaxValue;

            long minEtag = long.MaxValue;

            foreach (var lastEtagPerDestination in _lastSendEtagPerDestination)
            {
                replicationNodes.Remove(lastEtagPerDestination.Key);
                minEtag = Math.Min(lastEtagPerDestination.Value.LastEtag, minEtag);
            }
            if (replicationNodes.Count > 0)
            {
                // if we don't have information from all our destinations, we don't know what tombstones
                // we can remove. Note that this explicitly _includes_ disabled destinations, which prevents
                // us from doing any tombstone cleanup.
                return 0;
            }

            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var externals = _server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name).ExternalReplications;
                if (externals != null)
                {
                    foreach (var external in externals)
                    {
                        var state = GetExternalReplicationState(_server, Database.Name, external.TaskId, ctx);
                        var myEtag = ChangeVectorUtils.GetEtagById(state.DestinationChangeVector, Database.DbBase64Id);
                        minEtag = Math.Min(myEtag, minEtag);
                    }
                }
            }

            return minEtag;
        }

        public long GetMinimalEtagForTombstoneCleanupWithHubReplication()
        {
            long minEtag = long.MaxValue;

            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                if (_server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name).HubPullReplicationDefinitionExist() == false)
                    return minEtag;

                var time = Database.Configuration.Tombstones.CleanupIntervalWithReplicationHub.GetValue(TimeUnit.Minutes);
                var lastCleanUp = _hubInfoForCleaner?.LastCleanup ?? DateTime.MinValue;
                if (lastCleanUp.AddMinutes(time) > Database.Time.GetUtcNow())
                {
                    return _hubInfoForCleaner?.LastEtag ?? minEtag;
                }
            }

            long hoursToSave = Database.Configuration.Tombstones.RetentionTimeWithReplicationHub.GetValue(TimeUnit.Hours);

            var lastDateToSave = Database.Time.GetUtcNow().AddHours(-hoursToSave);

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (Database.DocumentsStorage.GetNumberOfTombstones(context) == 0)
                    return minEtag;
                var max = DocumentsStorage.ReadLastTombstoneEtag(context.Transaction.InnerTransaction);
                var min = _hubInfoForCleaner?.LastEtag ?? 0;
                var maxTombstone = Database.DocumentsStorage.GetTombstoneByEtag(context, max);

                if (maxTombstone.LastModified <= lastDateToSave)
                {
                    //All tombstones can be deleted
                    Interlocked.Exchange(ref _hubInfoForCleaner, new HubInfoForCleaner {LastCleanup = Database.Time.GetUtcNow(), LastEtag = max});
                    return max;
                }

                var minTombstone = Database.DocumentsStorage.GetTombstonesFrom(context, min, 0, 1).First();
                min = minTombstone.Etag;

                if (minTombstone.LastModified > lastDateToSave)
                {
                    // Can't delete tombstones yet
                    Interlocked.Exchange(ref _hubInfoForCleaner, new HubInfoForCleaner { LastCleanup = Database.Time.GetUtcNow(), LastEtag = minTombstone.Etag - 1 });
                    return minTombstone.Etag - 1;
                }
                while (true)
                {
                    var newEtag = ((max - min) / 2) + min;

                    var newTombstone = Database.DocumentsStorage.GetTombstonesFrom(context, newEtag, 0, 1).First();

                    if (newTombstone.Etag == max)
                    {
                        newTombstone = Database.DocumentsStorage.GetTombstoneAtOrBefore(context, newEtag);

                        if (newTombstone.Etag == min)
                        {
                            Interlocked.Exchange(ref _hubInfoForCleaner, new HubInfoForCleaner { LastCleanup = Database.Time.GetUtcNow(), LastEtag = min });
                            return min;
                        }
                    }
                    if (newTombstone.LastModified <= lastDateToSave)
                    {
                        min = newTombstone.Etag;
                        continue;
                    }
                    max = newTombstone.Etag;
                }
            }
        }


        private readonly Logger _log;

        // PERF: _incoming locks if you do _incoming.Values. Using .Select
        // directly and fetching the Value avoids this problem.
        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Select(x => x.Value.ConnectionInfo);

        public IEnumerable<ReplicationNode> OutgoingConnections => _outgoing.Select(x => x.Node);
        public IEnumerable<OutgoingReplicationHandler> OutgoingHandlers => _outgoing;

        // PERF: _incoming locks if you do _incoming.Values. Using .Select
        // directly and fetching the Value avoids this problem.
        public IEnumerable<IncomingReplicationHandler> IncomingHandlers => _incoming.Select(x => x.Value);

        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks =
            new ConcurrentQueue<TaskCompletionSource<object>>();

        internal readonly ServerStore _server;

        public List<ReplicationNode> Destinations => _destinations;
        private List<ReplicationNode> _destinations = new List<ReplicationNode>();
        private ClusterTopology _clusterTopology = new ClusterTopology();
        private int _numberOfSiblings;
        public ConflictSolver ConflictSolverConfig;

        public ReplicationLoader(DocumentDatabase database, ServerStore server)
        {
            _server = server;
            Database = database;
            var config = Database.Configuration.Replication;
            var reconnectTime = config.RetryReplicateAfter.AsTimeSpan;
            _log = LoggingSource.Instance.GetLogger<ReplicationLoader>(Database.Name);
            _reconnectAttemptTimer = new Timer(state => ForceTryReconnectAll(),
                null, reconnectTime, reconnectTime);
            MinimalHeartbeatInterval = (int)config.ReplicationMinimalHeartbeat.AsTimeSpan.TotalMilliseconds;
            database.TombstoneCleaner.Subscribe(this);
            server.Cluster.Changes.DatabaseChanged += DatabaseValueChanged;
        }

        private Task DatabaseValueChanged(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType changeType, object changeState)
        {
            var documentDatabase = Database;
            if (documentDatabase == null)
                return Task.CompletedTask;

            if (string.Equals(documentDatabase.Name, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                return Task.CompletedTask;

            switch (changeState)
            {
                case BulkRegisterReplicationHubAccessCommand bulk:
                    foreach (var cmd in bulk.Commands)
                    {
                        DisposeRelatedPullReplication(cmd.HubName, cmd.CertificateThumbprint);
                    }
                    break;
                case UpdatePullReplicationAsHubCommand put:
                    DisposeRelatedPullReplication(put.Definition.Name, null /*all*/);
                    break;
                case UnregisterReplicationHubAccessCommand del:
                    DisposeRelatedPullReplication(del.HubName, del.CertificateThumbprint);
                    break;
                case RegisterReplicationHubAccessCommand reg:
                    DisposeRelatedPullReplication(reg.HubName, reg.CertificateThumbprint);
                    break;
            }
            return Task.CompletedTask;

            void DisposeRelatedPullReplication(string hub, string certThumbprint)
            {
                if (hub == null)
                    return;

                foreach (var (_, repl) in _incoming)
                {
                    if (string.Equals(repl.PullReplicationName, hub, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (certThumbprint != null && repl.CertificateThumbprint != certThumbprint)
                        continue;

                    try
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info($"Resetting {repl.ConnectionInfo} for {hub} on {certThumbprint} because replication configuration changed. Will be reconnected.");
                        repl.Dispose();
                    }
                    catch
                    {
                    }
                }

                foreach (var repl in _outgoing)
                {
                    if (string.Equals(repl.PullReplicationDefinitionName, hub, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (certThumbprint != null && repl.CertificateThumbprint != certThumbprint)
                        continue;

                    try
                    {
                        repl.Dispose();
                    }
                    catch
                    {
                    }
                }
            }
        }

        public IReadOnlyDictionary<ReplicationNode, ConnectionShutdownInfo> OutgoingFailureInfo
            => _outgoingFailureInfo;

        public IReadOnlyDictionary<IncomingConnectionInfo, DateTime> IncomingLastActivityTime
            => _incomingLastActivityTime;

        public IReadOnlyDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>
            IncomingRejectionStats => _incomingRejectionStats;

        public IEnumerable<ReplicationNode> ReconnectQueue => _reconnectQueue.Select(x => x.Node);

        public long? GetLastReplicatedEtagForDestination(ReplicationNode dest)
        {
            foreach (var replicationHandler in _outgoing)
            {
                if (replicationHandler.Node.IsEqualTo(dest))
                    return replicationHandler._lastSentDocumentEtag;
            }
            return null;
        }

        public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions,
            TcpConnectionHeaderMessage header,
            X509Certificate2 certificate,
            JsonOperationContext.MemoryBuffer buffer)
        {
            var supportedVersions =
                TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Replication, tcpConnectionOptions.ProtocolVersion);

            ReplicationInitialRequest initialRequest = null;
            if (supportedVersions.Replication.PullReplication)
            {
                using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var readerObject = context.ParseToMemory(tcpConnectionOptions.Stream, "initial-replication-message",
                    BlittableJsonDocumentBuilder.UsageMode.None, buffer))
                {
                    initialRequest = JsonDeserializationServer.ReplicationInitialRequest(readerObject);
                }
            }

            string[] allowedPaths = default;
            string pullDefinitionName = null;
            switch (header.AuthorizeInfo?.AuthorizeAs)
            {
                case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                    if (supportedVersions.Replication.PullReplication == false)
                        throw new InvalidOperationException("Unable to use Pull Replication, because the other side doesn't have it as a supported feature");

                    if (header.AuthorizeInfo.AuthorizationFor == null)
                        throw new InvalidOperationException("Pull replication requires that the AuthorizationFor field will be set, but it wasn't provided");

                    PullReplicationDefinition pullReplicationDefinition;
                    using (_server.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        pullReplicationDefinition = _server.Cluster.ReadPullReplicationDefinition(Database.Name, header.AuthorizeInfo.AuthorizationFor, ctx);

                        if (pullReplicationDefinition.Disabled)
                            throw new InvalidOperationException("The replication hub " + pullReplicationDefinition.Name + " is disabled and cannot be used currently");
                    }

                    pullDefinitionName = header.AuthorizeInfo.AuthorizationFor;

                    switch (header.AuthorizeInfo.AuthorizeAs)
                    {
                        case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication:
                            if (pullReplicationDefinition.Mode.HasFlag(PullReplicationMode.HubToSink) == false)
                                throw new InvalidOperationException($"Replication hub {header.AuthorizeInfo.AuthorizationFor} does not support Pull Replication");
                            CreatePullReplicationAsHub(tcpConnectionOptions, initialRequest, supportedVersions, pullReplicationDefinition, header);
                            return;
                        case TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication:
                            if (pullReplicationDefinition.Mode.HasFlag(PullReplicationMode.SinkToHub) == false)
                                throw new InvalidOperationException($"Replication hub {header.AuthorizeInfo.AuthorizationFor} does not support Push Replication");
                            if (certificate == null)
                                throw new InvalidOperationException("Incoming filtered replication is only supported when using a certificate");
#pragma warning disable CS0618 // Type or member is obsolete
                            if (pullReplicationDefinition.Certificates != null && pullReplicationDefinition.Certificates.Count > 0)
#pragma warning restore CS0618 // Type or member is obsolete
                                throw new InvalidOperationException(
                                    "Incoming filtered replication is not supported on legacy replication hub. Make sure that there are no inline certificates on the replication hub: " +
                                    pullReplicationDefinition.Name);

                            allowedPaths = DetailedReplicationHubAccess.Preferred(header.ReplicationHubAccess.AllowedHubToSinkPaths, header.ReplicationHubAccess.AllowedSinkToHubPaths);

                            // same as normal incoming replication, just using the filtering
                            break;
                        default:
                            throw new InvalidOperationException("Unknown AuthroizeAs value: " + header.AuthorizeInfo.AuthorizeAs);
                    }
                    break;
                case null:
                    break;
                default:
                    throw new InvalidOperationException("Unknown AuthroizeAs value" + header.AuthorizeInfo?.AuthorizeAs);
            }

            CreateIncomingInstance(tcpConnectionOptions, allowedPaths, pullDefinitionName, buffer);
        }

        private void CreatePullReplicationAsHub(TcpConnectionOptions tcpConnectionOptions, ReplicationInitialRequest initialRequest,
                        TcpConnectionHeaderMessage.SupportedFeatures supportedVersions,
                        PullReplicationDefinition pullReplicationDefinition, TcpConnectionHeaderMessage header)
        {
            if (string.Equals(initialRequest.PullReplicationDefinitionName, pullReplicationDefinition.Name, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException(
                    $"PullReplicationDefinitionName '{initialRequest.PullReplicationDefinitionName}' does not match the pull replication definition name: {pullReplicationDefinition.Name}");

            var taskId = pullReplicationDefinition.TaskId; // every connection to this pull replication on the hub will have the same task id.
            var externalReplication = pullReplicationDefinition.ToExternalReplication(initialRequest, taskId);

            var outgoingReplication = new OutgoingReplicationHandler(null, this, Database, externalReplication, external: true, initialRequest.Info)
            {
                PullReplicationDefinitionName = initialRequest.PullReplicationDefinitionName,
                CertificateThumbprint = tcpConnectionOptions.Certificate?.Thumbprint
            };

            if (header.ReplicationHubAccess != null)
            {
                // Note that if the certificate isn't registered *specifically* in the pull replication, we don't do
                // any filtering. That means that the certificate has global access to the database, so there is not point
                outgoingReplication.PathsToSend = DetailedReplicationHubAccess.Preferred(header.ReplicationHubAccess.AllowedSinkToHubPaths, header.ReplicationHubAccess.AllowedHubToSinkPaths);
            }

            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            outgoingReplication.SuccessfulReplication += ResetReplicationFailuresInfo;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

            outgoingReplication.StartPullReplicationAsHub(tcpConnectionOptions.Stream, supportedVersions);
            OutgoingReplicationAdded?.Invoke(outgoingReplication);
        }

        public void RunPullReplicationAsSink(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer, PullReplicationAsSink destination)
        {
            string[] allowedPaths = DetailedReplicationHubAccess.Preferred(destination.AllowedHubToSinkPaths, destination.AllowedSinkToHubPaths);
            var pullParams = new IncomingPullReplicationParams
            {
                Name = destination.HubName,
                AllowedPaths = allowedPaths,
                Mode = PullReplicationMode.HubToSink
            };
            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, pullParams);
            newIncoming.Failed += RetryPullReplication;

            PoolOfThreads.PooledThread.ResetCurrentThreadName();
            Thread.CurrentThread.Name = $"Pull Replication as Sink from {destination.Database} at {destination.Url}";

            _incoming[newIncoming.ConnectionInfo.SourceDatabaseId] = newIncoming;
            IncomingReplicationAdded?.Invoke(newIncoming);
            newIncoming.DoIncomingReplication();

            void RetryPullReplication(IncomingReplicationHandler instance, Exception e)
            {
                using (instance)
                {
                    if (_incoming.TryRemove(instance.ConnectionInfo.SourceDatabaseId, out _))
                        IncomingReplicationRemoved?.Invoke(instance);

                    instance.Failed -= RetryPullReplication;
                    instance.DocumentsReceived -= OnIncomingReceiveSucceeded;
                    if (_log.IsInfoEnabled)
                        _log.Info($"Pull replication Sink handler has thrown an unhandled exception. ({instance.FromToString})", e);
                }

                // if the stream closed, it is our duty to reconnect
                AddAndStartOutgoingReplication(destination, true);
            }
        }

        private void CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, string[] allowedPaths, string pullReplicationName,
                        JsonOperationContext.MemoryBuffer buffer)
        {
            IncomingPullReplicationParams pullParams = null;
            if (pullReplicationName != null)
            {
                pullParams = new IncomingPullReplicationParams
                {
                    Name = pullReplicationName,
                    AllowedPaths = allowedPaths,
                    Mode = PullReplicationMode.SinkToHub
                };
            }
            
            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, pullParams);
            newIncoming.Failed += OnIncomingReceiveFailed;

            // need to safeguard against two concurrent connection attempts
            var current = _incoming.AddOrUpdate(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming,
                (_, val) => val.IsDisposed ? newIncoming : val);

            if (current == newIncoming)
            {
                newIncoming.Start();
                IncomingReplicationAdded?.Invoke(newIncoming);
                ForceTryReconnectAll();
            }
            else
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("you can't add two identical connections.", new InvalidOperationException("you can't add two identical connections."));
                }
                newIncoming.Dispose();
            }
        }

        public class IncomingPullReplicationParams
        {
            public string Name;
            public string[] AllowedPaths;
            public PullReplicationMode Mode;
        }

        private IncomingReplicationHandler CreateIncomingReplicationHandler(
            TcpConnectionOptions tcpConnectionOptions,
            JsonOperationContext.MemoryBuffer buffer,
            IncomingPullReplicationParams incomingPullParams)
        {
            var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, incomingPullParams?.AllowedPaths, buffer);

            var newIncoming = new IncomingReplicationHandler(
                tcpConnectionOptions,
                getLatestEtagMessage,
                this,
                buffer,
                incomingPullParams);

            newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
            return newIncoming;
        }

        private ReplicationLatestEtagRequest IncomingInitialHandshake(TcpConnectionOptions tcpConnectionOptions, string[] incomingPaths, JsonOperationContext.MemoryBuffer buffer)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;

            using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var readerObject = context.ParseToMemory(
                tcpConnectionOptions.Stream,
                "IncomingReplication/get-last-etag-message read",
                BlittableJsonDocumentBuilder.UsageMode.None,
                buffer))
            {
                var exceptionSchema = JsonDeserializationClient.ExceptionSchema(readerObject);
                if (exceptionSchema.Type.Equals("Error"))
                    throw new Exception(exceptionSchema.Message);

                getLatestEtagMessage = JsonDeserializationServer.ReplicationLatestEtagRequest(readerObject);
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"GetLastEtag: {getLatestEtagMessage.SourceTag}({getLatestEtagMessage.SourceMachineName}) / {getLatestEtagMessage.SourceDatabaseName} ({getLatestEtagMessage.SourceDatabaseId}) - {getLatestEtagMessage.SourceUrl}");
                }
            }

            var connectionInfo = IncomingConnectionInfo.FromGetLatestEtag(getLatestEtagMessage);
            try
            {
                AssertValidConnection(connectionInfo);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Connection from [{connectionInfo}] is rejected.", e);

                var incomingConnectionRejectionInfos = _incomingRejectionStats.GetOrAdd(connectionInfo,
                    _ => new ConcurrentQueue<IncomingConnectionRejectionInfo>());
                incomingConnectionRejectionInfos.Enqueue(new IncomingConnectionRejectionInfo { Reason = e.ToString() });

                throw;
            }

            try
            {
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsOperationContext))
                using (Database.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationContext))
                using (var writer = new BlittableJsonTextWriter(documentsOperationContext, tcpConnectionOptions.Stream))
                using (documentsOperationContext.OpenReadTransaction())
                using (configurationContext.OpenReadTransaction())
                {
                    var changeVector = DocumentsStorage.GetDatabaseChangeVector(documentsOperationContext);

                    var lastEtagFromSrc = DocumentsStorage.GetLastReplicatedEtagFrom(
                        documentsOperationContext, getLatestEtagMessage.SourceDatabaseId);
                    if (_log.IsInfoEnabled)
                        _log.Info($"GetLastEtag response, last etag: {lastEtagFromSrc}");
                    var response = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = nameof(ReplicationMessageReply.ReplyType.Ok),
                        [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtagFromSrc,
                        [nameof(ReplicationMessageReply.NodeTag)] = _server.NodeTag,
                        [nameof(ReplicationMessageReply.DatabaseChangeVector)] = changeVector,
                        [nameof(ReplicationMessageReply.AcceptablePaths)] = incomingPaths,
                    };

                    documentsOperationContext.Write(writer, response);
                    writer.Flush();
                }
            }
            catch (Exception)
            {
                try
                {
                    tcpConnectionOptions.Dispose();
                }
                catch (Exception)
                {
                    // do nothing
                }

                throw;
            }

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}");

            return getLatestEtagMessage;
        }

        private long _reconnectInProgress;

        private void ForceTryReconnectAll()
        {
            if (Interlocked.CompareExchange(ref _reconnectInProgress, 1, 0) == 1)
                return;

            try
            {
                foreach (var failure in _reconnectQueue)
                {
                    try
                    {
                        if (_reconnectQueue.TryRemove(failure) == false)
                            continue;

                        if (_outgoingFailureInfo.Values.Contains(failure) == false)
                            continue; // this connection is no longer exists

                        if (failure.RetryOn > DateTime.UtcNow)
                        {
                            _reconnectQueue.Add(failure);
                            continue;
                        }

                        AddAndStartOutgoingReplication(failure.Node, failure.External);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Failed to start outgoing replication to {failure.Node}", e);
                        }
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref _reconnectInProgress, 0);
            }
        }

        internal static readonly TimeSpan MaxInactiveTime = TimeSpan.FromSeconds(60);

        private void AssertValidConnection(IncomingConnectionInfo connectionInfo)
        {
            //precaution, should never happen..
            if (string.IsNullOrWhiteSpace(connectionInfo.SourceDatabaseId) ||
                Guid.TryParse(connectionInfo.SourceDatabaseId, out Guid sourceDbId) == false)
            {
                throw new InvalidOperationException(
                    $"Failed to parse source database Id. What I got is {(string.IsNullOrWhiteSpace(connectionInfo.SourceDatabaseId) ? "<empty string>" : Database.DbId.ToString())}. This is not supposed to happen and is likely a bug.");
            }

            if (sourceDbId == Database.DbId)
            {
                throw new InvalidOperationException(
                    $"Cannot have replication with source and destination being the same database. They share the same db id ({connectionInfo} - {Database.DbId})");
            }

            if (_server.IsPassive())
            {
                throw new InvalidOperationException(
                    $"Cannot accept the incoming replication connection from {connectionInfo.SourceUrl}, because this node is in passive state.");
            }

            if (_incoming.TryGetValue(connectionInfo.SourceDatabaseId, out var value))
            {
                var lastHeartbeat = new DateTime(value.LastHeartbeatTicks);
                if (lastHeartbeat + MaxInactiveTime > Database.Time.GetUtcNow())
                    throw new InvalidOperationException(
                        $"An active connection for this database already exists from {value.ConnectionInfo.SourceUrl} (last heartbeat: {lastHeartbeat}).");

                if (_log.IsInfoEnabled)
                    _log.Info($"Disconnecting existing connection from {value.FromToString} because we got a new connection from the same source db " +
                              $"(last heartbeat was at {lastHeartbeat}).");

                IncomingReplicationRemoved?.Invoke(value);

                value.Dispose();
            }
        }

        public ClusterTopology GetClusterTopology()
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                return _server.GetClusterTopology(ctx);
            }
        }

        public void Initialize(DatabaseRecord record)
        {
            if (_isInitialized) //precaution -> probably not necessary, but still...
                return;

            ConflictSolverConfig = record.ConflictSolverConfig;
            ConflictResolver = new ResolveConflictOnReplicationConfigurationChange(this, _log);
            ConflictResolver.RunConflictResolversOnce();
            _isInitialized.Raise();
        }

        public void HandleDatabaseRecordChange(DatabaseRecord newRecord)
        {
            HandleConflictResolverChange(newRecord);
            HandleTopologyChange(newRecord);
            UpdateConnectionStrings(newRecord);
        }

        private void UpdateConnectionStrings(DatabaseRecord newRecord)
        {
            if (newRecord == null)
            {
                // we drop the connections in the handle topology change method
                return;
            }
            foreach (var connection in OutgoingFailureInfo)
            {
                if (connection.Key is ExternalReplication external)
                {
                    if (ValidateConnectionString(newRecord, external, out var connectionString))
                    {
                        external.ConnectionString = connectionString;
                    }
                }
            }
        }

        private void HandleConflictResolverChange(DatabaseRecord newRecord)
        {
            if (newRecord == null)
            {
                ConflictSolverConfig = null;
                return;
            }

            if (ConflictSolverConfig == null && newRecord.ConflictSolverConfig == null)
            {
                return;
            }

            var conflictSolverChanged = ConflictSolverConfig?.ConflictResolutionChanged(newRecord.ConflictSolverConfig) ?? true;
            if (conflictSolverChanged)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Conflict resolution was change.");
                }
                ConflictSolverConfig = newRecord.ConflictSolverConfig;
                ConflictResolver.RunConflictResolversOnce();
            }
        }

        private void HandleTopologyChange(DatabaseRecord newRecord)
        {
            var instancesToDispose = new List<IDisposable>();
            if (newRecord == null || _server.IsPassive())
            {
                DropOutgoingConnections(Destinations, instancesToDispose);
                DropIncomingConnections(Destinations, instancesToDispose);
                _internalDestinations.Clear();
                _externalDestinations.Clear();
                _destinations.Clear();
                DisposeConnections(instancesToDispose);
                return;
            }

            _clusterTopology = GetClusterTopology();

            HandleInternalReplication(newRecord, instancesToDispose);
            HandleExternalReplication(newRecord, instancesToDispose);
            HandleHubPullReplication(newRecord, instancesToDispose);
            var destinations = new List<ReplicationNode>();
            destinations.AddRange(_internalDestinations);
            destinations.AddRange(_externalDestinations);
            _destinations = destinations;
            _numberOfSiblings = _destinations.Select(x => x.Url).Intersect(_clusterTopology.AllNodes.Select(x => x.Value)).Count();

            DisposeConnections(instancesToDispose);
        }

        private void HandleHubPullReplication(DatabaseRecord newRecord, List<IDisposable> instancesToDispose)
        {
            foreach (var instance in OutgoingHandlers)
            {
                if (instance.PullReplicationDefinitionName == null)
                    continue;

                var pullReplication = newRecord.HubPullReplications.Find(x => x.Name == instance.PullReplicationDefinitionName);

                if (pullReplication != null && pullReplication.Disabled == false)
                {
                    // update the destination
                    var current = instance.Destination as ExternalReplication;
                    if (current.DelayReplicationFor != pullReplication.DelayReplicationFor)
                    {
                        current.DelayReplicationFor = pullReplication.DelayReplicationFor;
                        instance.NextReplicateTicks = 0;
                    }
                    current.MentorNode = pullReplication.MentorNode;
                    continue;
                }

                if (_log.IsInfoEnabled)
                    _log.Info($"Stopping replication to {instance.Destination.FromString()}");

                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.SuccessfulReplication -= ResetReplicationFailuresInfo;
                instancesToDispose.Add(instance);
                _outgoing.TryRemove(instance);
                _lastSendEtagPerDestination.TryRemove(instance.Destination, out LastEtagPerDestination _);
                _outgoingFailureInfo.TryRemove(instance.Destination, out ConnectionShutdownInfo info);
                if (info != null)
                    _reconnectQueue.TryRemove(info);
            }
        }

        private void DropIncomingConnections(IEnumerable<ReplicationNode> connectionsToRemove, List<IDisposable> instancesToDispose)
        {
            var toRemove = connectionsToRemove?.ToList();
            if (toRemove == null || toRemove.Count == 0)
                return;

            // this is relevant for sink
            foreach (var incoming in _incoming)
            {
                var instance = incoming.Value;
                if (toRemove.Any(conn => conn.Url == instance.ConnectionInfo.SourceUrl))
                {
                    if (_incoming.TryRemove(instance.ConnectionInfo.SourceDatabaseId, out _))
                        IncomingReplicationRemoved?.Invoke(instance);

                    instance.ClearEvents();
                    instancesToDispose.Add(instance);
                }
            }
        }

        private void DisposeConnections(List<IDisposable> instancesToDispose)
        {
            TaskExecutor.Execute(toDispose =>
            {
                Parallel.ForEach((List<IDisposable>)toDispose, instance =>
                {
                    try
                    {
                        instance?.Dispose();
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            switch (instance)
                            {
                                case OutgoingReplicationHandler outHandler:
                                    _log.Info($"Failed to dispose outgoing replication to {outHandler.DestinationFormatted}", e);
                                    break;
                                case IncomingReplicationHandler inHandler:
                                    _log.Info($"Failed to dispose incoming replication to {inHandler.SourceFormatted}", e);
                                    break;
                                default:
                                    _log.Info($"Failed to dispose an unknown type '{instance?.GetType()}", e);
                                    break;
                            }
                        }
                    }
                });
            }, instancesToDispose);
        }

        private (List<ExternalReplicationBase> AddedDestinations, List<ExternalReplicationBase> RemovedDestiantions) FindExternalReplicationChanges(
            HashSet<ExternalReplicationBase> current,
            List<ExternalReplicationBase> newDestinations)
        {
            if (newDestinations == null)
                newDestinations = new List<ExternalReplicationBase>();

            var outgoingHandlers = OutgoingHandlers.ToList();

            var addedDestinations = new List<ExternalReplicationBase>();
            var removedDestinations = current.ToList();
            foreach (var newDestination in newDestinations.ToArray())
            {
                if (newDestination.Disabled)
                    continue;

                removedDestinations.Remove(newDestination);
                if (current.TryGetValue(newDestination, out var actual))
                {
                    // if we update the delay we don't want to break the replication (the hash code will be the same),
                    // but we need to update the Destination instance

                    // ReSharper disable once PossibleUnintendedReferenceComparison
                    var handler = outgoingHandlers.Find(o => o.Destination == actual); // we explicitly compare references.
                    if (handler == null)
                        continue;

                    if (handler.Destination is ExternalReplication ex &&
                        actual is ExternalReplication actualEx &&
                        newDestination is ExternalReplication newDestinationEx)
                    {
                        if (ex.DelayReplicationFor != actualEx.DelayReplicationFor)
                            handler.NextReplicateTicks = 0;

                        ex.DelayReplicationFor = newDestinationEx.DelayReplicationFor;
                        ex.MentorNode = newDestination.MentorNode;
                    }
                    continue;
                }

                addedDestinations.Add(newDestination);
            }

            return (addedDestinations, removedDestinations);
        }

        private void HandleExternalReplication(DatabaseRecord newRecord, List<IDisposable> instancesToDispose)
        {
            var externalReplications = newRecord.ExternalReplications.Concat<ExternalReplicationBase>(newRecord.SinkPullReplications).ToList();
            SetExternalReplicationProperties(newRecord, externalReplications);

            var changes = FindExternalReplicationChanges(_externalDestinations, externalReplications);

            DropOutgoingConnections(changes.RemovedDestiantions, instancesToDispose);
            DropIncomingConnections(changes.RemovedDestiantions, instancesToDispose);

            var newDestinations = GetMyNewDestinations(newRecord, changes.AddedDestinations);
            if (newDestinations.Count > 0)
            {
                Task.Run(() =>
                {
                    // here we might have blocking calls to fetch the tcp info.
                    try
                    {
                        StartOutgoingConnections(newDestinations, external: true);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsOperationsEnabled)
                            _log.Operations($"Failed to start the outgoing connections to {newDestinations.Count} new destinations", e);
                    }
                });
            }

            _externalDestinations.RemoveWhere(changes.RemovedDestiantions.Contains);
            foreach (var newDestination in newDestinations)
            {
                _externalDestinations.Add(newDestination);
            }
        }

        private void SetExternalReplicationProperties(DatabaseRecord newRecord, List<ExternalReplicationBase> externalReplications)
        {
            for (var i = 0; i < externalReplications.Count; i++)
            {
                var externalReplication = externalReplications[i];
                if (ValidateConnectionString(newRecord, externalReplication, out var connectionString) == false)
                {
                    continue;
                }

                externalReplication.Database = connectionString.Database;
                externalReplication.ConnectionString = connectionString;

                if (externalReplication is PullReplicationAsSink sink &&
                    sink.Mode == (PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink))
                {
                    // we have dual mode here, need to split it
                    sink.Mode = PullReplicationMode.SinkToHub;

                    var other = new PullReplicationAsSink
                    {
                        Database = sink.Database,
                        Disabled = sink.Disabled,
                        AllowedHubToSinkPaths = sink.AllowedHubToSinkPaths,
                        Mode = PullReplicationMode.HubToSink,
                        Name = sink.Name,
                        Url = sink.Url,
                        ConnectionString = sink.ConnectionString,
                        CertificatePassword = sink.CertificatePassword,
                        AllowedSinkToHubPaths = sink.AllowedSinkToHubPaths,
                        MentorNode = sink.MentorNode,
                        TaskId = sink.TaskId,
                        ConnectionStringName = sink.ConnectionStringName,
                        HubName = sink.HubName,
                        CertificateWithPrivateKey = sink.CertificateWithPrivateKey
                    };

                    i += 1;
                    externalReplications.Insert(i, other);
                }
            }
        }

        private List<ExternalReplicationBase> GetMyNewDestinations(DatabaseRecord newRecord, List<ExternalReplicationBase> added)
        {
            return added.Where(configuration =>
            {
                if (ValidateConnectionString(newRecord, configuration, out _) == false)
                    return false;

                var taskStatus = GetExternalReplicationState(_server, Database.Name, configuration.TaskId);
                var whoseTaskIsIt = Database.WhoseTaskIsIt(newRecord.Topology, configuration, taskStatus);
                return whoseTaskIsIt == _server.NodeTag;
            }).ToList();
        }

        public static ExternalReplicationState GetExternalReplicationState(ServerStore server, string database, long taskId)
        {
            using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetExternalReplicationState(server, database, taskId, context);
            }
        }

        private static ExternalReplicationState GetExternalReplicationState(ServerStore server, string database, long taskId, TransactionOperationContext context)
        {
            var stateBlittable = server.Cluster.Read(context, ExternalReplicationState.GenerateItemName(database, taskId));

            return stateBlittable != null ? JsonDeserializationCluster.ExternalReplicationState(stateBlittable) : new ExternalReplicationState();
        }

        public void EnsureNotDeleted(string node)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name))
                {
                    if (rawRecord != null && rawRecord.DeletionInProgress.ContainsKey(node))
                    {
                        throw new OperationCanceledException($"The database '{Database.Name}' on node '{node}' is being deleted, so it will not handle replications.");
                    }
                }
            }
        }

        public void CompleteDeletionIfNeeded(CancellationTokenSource cts)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name))
            {
                if (rawRecord == null)
                    return;

                var deletionInProgress = rawRecord.DeletionInProgress;
                if (deletionInProgress.ContainsKey(_server.NodeTag) == false)
                    return;

                try
                {
                    cts.Cancel();
                }
                catch
                {
                    // nothing that we can do about it.
                    // probably the database is being deleted.
                }
                finally
                {
                    var record = rawRecord.MaterializedRecord;
                    TaskExecutor.Execute(_ => _server.DatabasesLandlord.DeleteDatabase(Database.Name, deletionInProgress[_server.NodeTag], record), null);
                }
            }
        }

        private bool ValidateConnectionString(DatabaseRecord newRecord, ExternalReplicationBase externalReplication, out RavenConnectionString connectionString)
        {
            connectionString = null;
            if (string.IsNullOrEmpty(externalReplication.ConnectionStringName))
            {
                var msg = $"The external replication {externalReplication.Name} to the database '{externalReplication.Database}' " +
                          "has an empty connection string name.";

                if (_log.IsInfoEnabled)
                {
                    _log.Info(msg);
                }

                _server.NotificationCenter.Add(AlertRaised.Create(
                    Database.Name,
                    "Connection string name is empty",
                    msg,
                    AlertType.Replication,
                    NotificationSeverity.Error));
                return false;
            }

            if (newRecord.RavenConnectionStrings.TryGetValue(externalReplication.ConnectionStringName, out connectionString) == false)
            {
                var msg = $"Could not find connection string with name {externalReplication.ConnectionStringName} " +
                          $"for the external replication task '{externalReplication.Name}' to '{externalReplication.Database}'.";

                if (_log.IsInfoEnabled)
                {
                    _log.Info(msg);
                }

                _server.NotificationCenter.Add(AlertRaised.Create(
                    Database.Name,
                    "Connection string not found",
                    msg,
                    AlertType.Replication,
                    NotificationSeverity.Error));

                return false;
            }
            return true;
        }

        private void HandleInternalReplication(DatabaseRecord newRecord, List<IDisposable> instancesToDispose)
        {
            var newInternalDestinations =
                newRecord.Topology?.GetDestinations(_server.NodeTag, Database.Name, newRecord.DeletionInProgress, _clusterTopology, _server.Engine.CurrentState);
            var internalConnections = DatabaseTopology.FindChanges(_internalDestinations, newInternalDestinations);

            if (internalConnections.RemovedDestiantions.Count > 0)
            {
                var removed = internalConnections.RemovedDestiantions.Select(r => new InternalReplication
                {
                    NodeTag = _clusterTopology.TryGetNodeTagByUrl(r).NodeTag,
                    Url = r,
                    Database = Database.Name
                });

                DropOutgoingConnections(removed, instancesToDispose);
            }
            if (internalConnections.AddedDestinations.Count > 0)
            {
                var added = internalConnections.AddedDestinations.Select(r => new InternalReplication
                {
                    NodeTag = _clusterTopology.TryGetNodeTagByUrl(r).NodeTag,
                    Url = r,
                    Database = Database.Name
                });
                StartOutgoingConnections(added.ToList());
            }
            _internalDestinations.Clear();
            foreach (var item in newInternalDestinations)
            {
                _internalDestinations.Add(item);
            }
        }

        private void StartOutgoingConnections(IReadOnlyCollection<ReplicationNode> connectionsToAdd, bool external = false)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Initializing {connectionsToAdd.Count:#,#} outgoing replications from {Database} on {_server.NodeTag}.");

            foreach (var destination in connectionsToAdd)
            {
                if (destination.Disabled)
                    continue;

                if (_log.IsInfoEnabled)
                    _log.Info("Initialized outgoing replication for " + destination.FromString());
                AddAndStartOutgoingReplication(destination, external);
            }

            if (_log.IsInfoEnabled)
                _log.Info("Finished initialization of outgoing replications..");
        }

        private void DropOutgoingConnections(IEnumerable<ReplicationNode> connectionsToRemove, List<IDisposable> instancesToDispose)
        {
            var toRemove = connectionsToRemove.ToList();
            foreach (var replication in _reconnectQueue.ToList())
            {
                if (toRemove.Contains(replication.Node))
                {
                    _reconnectQueue.TryRemove(replication);
                }
            }

            var outgoingChanged = _outgoing.Where(o => toRemove.Contains(o.Destination)).ToList();
            if (outgoingChanged.Count == 0)
                return; // no connections to remove

            if (_log.IsInfoEnabled)
                _log.Info($"Dropping {outgoingChanged.Count:#,#} outgoing replications connections from {Database} on {_server.NodeTag}.");

            foreach (var instance in outgoingChanged)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Stopping replication to {instance.Destination.FromString()}");

                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.SuccessfulReplication -= ResetReplicationFailuresInfo;
                instancesToDispose.Add(instance);
                _outgoing.TryRemove(instance);
                _lastSendEtagPerDestination.TryRemove(instance.Destination, out LastEtagPerDestination _);
                _outgoingFailureInfo.TryRemove(instance.Destination, out ConnectionShutdownInfo info);
                if (info != null)
                    _reconnectQueue.TryRemove(info);
            }
        }

        internal void AddAndStartOutgoingReplication(ReplicationNode node, bool external)
        {
            var info = GetConnectionInfo(node, external);

            if (info == null)
            {
                // this means that we were unable to retrieve the tcp connection info and will try it again later
                return;
            }

            if (_locker.TryEnterReadLock(0) == false)
            {
                // the db being disposed
                return;
            }

            try
            {
                if (Database == null)
                    return;

                var outgoingReplication = new OutgoingReplicationHandler(null, this, Database, node, external, info);
                if (node is PullReplicationAsSink sink)
                {
                    outgoingReplication.PathsToSend = DetailedReplicationHubAccess.Preferred(sink.AllowedSinkToHubPaths, sink.AllowedHubToSinkPaths);
                }
                outgoingReplication.Failed += OnOutgoingSendingFailed;
                outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
                outgoingReplication.SuccessfulReplication += ResetReplicationFailuresInfo;
                _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

                outgoingReplication.Start();

                OutgoingReplicationAdded?.Invoke(outgoingReplication);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        private TcpConnectionInfo GetConnectionInfo(ReplicationNode node, bool external)
        {
            var shutdownInfo = _outgoingFailureInfo.GetOrAdd(node, new ConnectionShutdownInfo
            {
                Node = node,
                External = external,
                MaxConnectionTimeout = Database.Configuration.Replication.RetryMaxTimeout.AsTimeSpan.TotalMilliseconds
            });

            X509Certificate2 certificate = null;
            try
            {
                certificate = GetCertificateForReplication(node, out _);

                switch (node)
                {
                    case ExternalReplicationBase exNode:
                        {
                            var database = exNode.ConnectionString.Database;
                            if (node is PullReplicationAsSink sink)
                            {
                                return GetPullReplicationTcpInfo(sink, certificate, database);
                            }

                            // normal external replication
                            return GetExternalReplicationTcpInfo(exNode as ExternalReplication, certificate, database);
                        }
                    case InternalReplication internalNode:
                        {
                            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown))
                            {
                                cts.CancelAfter(_server.Engine.TcpConnectionTimeout);
                                return ReplicationUtils.GetTcpInfo(internalNode.Url, internalNode.Database, Database.DbId.ToString(), Database.ReadLastEtag(),
                                    "Replication",
                                    certificate, cts.Token);
                            }
                        }
                    default:
                        throw new InvalidOperationException(
                            $"Unexpected replication node type, Expected to be '{typeof(ExternalReplication)}' or '{typeof(InternalReplication)}', but got '{node.GetType()}'");
                }
            }
            catch (Exception e)
            {
                if (Database.DatabaseShutdown.IsCancellationRequested)
                    return null;

                // will try to fetch it again later
                if (_log.IsInfoEnabled)
                {
                    if (e is DatabaseIdleException)
                    {
                        // this is expected, so we don't mark it as error
                        _log.Info($"The database is idle on the destination '{node.FromString()}', the connection will be retried later.");
                    }
                    else
                    {
                        _log.Info($"Failed to fetch tcp connection information for the destination '{node.FromString()}' , the connection will be retried later.", e);
                    }
                }

                if (e is AuthorizationException)
                {
                    var alert = AlertRaised.Create(
                        node.Database,
                        $"Forbidden access to {node.FromString()}'",
                        $"Replication failed. Certificate : {certificate?.FriendlyName} does not have permission to access or is unknown.",
                        AlertType.Replication,
                        NotificationSeverity.Error);

                        _server.NotificationCenter.Add(alert);
                }

                var replicationPulse = new LiveReplicationPulsesCollector.ReplicationPulse
                {
                    OccurredAt = SystemTime.UtcNow,
                    Direction = ReplicationPulseDirection.OutgoingGetTcpInfo,
                    To = node,
                    IsExternal = external,
                    ExceptionMessage = e.Message,
                };
                OutgoingReplicationConnectionFailed?.Invoke(replicationPulse);

                if (node is PullReplicationAsSink)
                {
                    var stats = new IncomingReplicationStatsAggregator(GetNextReplicationStatsId(), null);
                    using (var scope = stats.CreateScope())
                    {
                        scope.AddError(e);
                    }

                    var failureReporter = new IncomingReplicationFailureToConnectReporter(node, stats);
                    IncomingReplicationConnectionErrored?.Invoke(node, failureReporter);
                    IncomingConnectionsLastFailureToConnect.AddOrUpdate(node, failureReporter, (_, __) => failureReporter);
                }
                else
                {
                    var stats = new OutgoingReplicationStatsAggregator(GetNextReplicationStatsId(), null);
                    using (var scope = stats.CreateScope())
                    {
                        scope.AddError(e);
                    }

                    var failureReporter = new OutgoingReplicationFailureToConnectReporter(node, stats);
                    OutgoingReplicationConnectionErrored?.Invoke(node, failureReporter);
                    OutgoingConnectionsLastFailureToConnect.AddOrUpdate(node, failureReporter, (_, __) => failureReporter);
                }

                shutdownInfo.OnError(e);
                _reconnectQueue.TryAdd(shutdownInfo);
            }

            return null;
        }

        public ConcurrentDictionary<ReplicationNode, OutgoingReplicationFailureToConnectReporter> OutgoingConnectionsLastFailureToConnect =
            new ConcurrentDictionary<ReplicationNode, OutgoingReplicationFailureToConnectReporter>();

        public ConcurrentDictionary<ReplicationNode, IncomingReplicationFailureToConnectReporter> IncomingConnectionsLastFailureToConnect =
            new ConcurrentDictionary<ReplicationNode, IncomingReplicationFailureToConnectReporter>();

        private TcpConnectionInfo GetPullReplicationTcpInfo(PullReplicationAsSink pullReplicationAsSink, X509Certificate2 certificate, string database)
        {
            var remoteTask = pullReplicationAsSink.HubName;
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                string[] remoteDatabaseUrls;
                // fetch hub cluster node urls
                using (var requestExecutor = RequestExecutor.CreateForFixedTopology(pullReplicationAsSink.ConnectionString.TopologyDiscoveryUrls, pullReplicationAsSink.ConnectionString.Database,
                    certificate, DocumentConventions.DefaultForServer))
                {
                    var cmd = new GetRemoteTaskTopologyCommand(database, Database.DatabaseGroupId, remoteTask);

                    try
                    {
                        requestExecutor.Execute(cmd, ctx, token: Database.DatabaseShutdown);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info($"Failed to execute {nameof(GetRemoteTaskTopologyCommand)} for {pullReplicationAsSink.Name}", e);

                        // failed to connect, will retry later
                        throw;
                    }
                    finally
                    {
                        // we want to set node Url even if we fail to connect to destination, so they can be used in replication stats
                        pullReplicationAsSink.Url = requestExecutor.Url;
                        pullReplicationAsSink.Database = database;
                    }

                    remoteDatabaseUrls = cmd.Result;
                }

                // fetch tcp info for the hub nodes
                using (var requestExecutor = RequestExecutor.CreateForFixedTopology(remoteDatabaseUrls,
                    pullReplicationAsSink.ConnectionString.Database, certificate, DocumentConventions.DefaultForServer))
                {
                    var cmd = new GetTcpInfoForRemoteTaskCommand(ExternalReplicationTag, database, remoteTask);

                    try
                    {
                        requestExecutor.Execute(cmd, ctx, token: Database.DatabaseShutdown);
                    }
                    finally
                    {
                        pullReplicationAsSink.Url = requestExecutor.Url;
                        pullReplicationAsSink.Database = database;
                    }

                    return cmd.Result;
                }
            }
        }

        private static readonly string ExternalReplicationTag = "external-replication";

        private TcpConnectionInfo GetExternalReplicationTcpInfo(ExternalReplication exNode, X509Certificate2 certificate, string database)
        {
            using (var requestExecutor = RequestExecutor.Create(exNode.ConnectionString.TopologyDiscoveryUrls, exNode.ConnectionString.Database, certificate, DocumentConventions.DefaultForServer))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var cmd = new GetTcpInfoCommand(ExternalReplicationTag, database, Database.DbId.ToString(), Database.ReadLastEtag());
                try
                {
                    requestExecutor.Execute(cmd, ctx, token: Database.DatabaseShutdown);
                }
                finally
                {
                    // we want to set node Url even if we fail to connect to destination, so they can be used in replication stats
                    exNode.Database = database;
                    exNode.Url = requestExecutor.Url;
                }

                return cmd.Result;
            }
        }

        public X509Certificate2 GetCertificateForReplication(ReplicationNode node, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            switch (node)
            {
                case InternalReplication _:
                case ExternalReplication _:
                    authorizationInfo = null;
                    return _server.Server.Certificate.Certificate;
                case PullReplicationAsSink sink:
                    authorizationInfo = new TcpConnectionHeaderMessage.AuthorizationInfo
                    {
                        AuthorizeAs = sink.Mode switch
                        {
                            PullReplicationMode.HubToSink => TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication,
                            PullReplicationMode.SinkToHub => TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PushReplication,
                            PullReplicationMode.None => throw new ArgumentOutOfRangeException(nameof(node), "Replication mode should be set to pull or push"),
                            _ => throw new ArgumentOutOfRangeException("Unexpected replication mode: " + sink.Mode)
                        },
                        AuthorizationFor = sink.HubName
                    };

                    if (sink.CertificateWithPrivateKey == null)
                        return _server.Server.Certificate.Certificate;

                    var certBytes = Convert.FromBase64String(sink.CertificateWithPrivateKey);
                    return new X509Certificate2(certBytes, sink.CertificatePassword, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                default:
                    throw new ArgumentException($"Unknown node type {node.GetType().FullName}");
            }
        }

        public (string Url, OngoingTaskConnectionStatus Status) GetExternalReplicationDestination(long taskId)
        {
            foreach (var outgoing in OutgoingConnections)
            {
                if (outgoing is ExternalReplication ex && ex.TaskId == taskId)
                    return (ex.Url, OngoingTaskConnectionStatus.Active);
            }
            foreach (var reconnect in ReconnectQueue)
            {
                if (reconnect is ExternalReplication ex && ex.TaskId == taskId)
                    return (ex.Url, OngoingTaskConnectionStatus.Reconnect);
            }
            return (null, OngoingTaskConnectionStatus.NotActive);
        }

        private void OnIncomingReceiveFailed(IncomingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                if (_incoming.TryRemove(instance.ConnectionInfo.SourceDatabaseId, out _))
                    IncomingReplicationRemoved?.Invoke(instance);

                instance.Failed -= OnIncomingReceiveFailed;
                instance.DocumentsReceived -= OnIncomingReceiveSucceeded;

                if (_log.IsInfoEnabled)
                    _log.Info($"Incoming replication handler has thrown an unhandled exception. ({instance.FromToString})", e);
            }
        }

        private void OnOutgoingSendingFailed(OutgoingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.SuccessfulReplication -= ResetReplicationFailuresInfo;

                _outgoing.TryRemove(instance);
                OutgoingReplicationRemoved?.Invoke(instance);

                if (instance.IsPullReplicationAsHub)
                    _externalDestinations.Remove(instance.Destination as ExternalReplication);

                if (_outgoingFailureInfo.TryGetValue(instance.Node, out ConnectionShutdownInfo failureInfo) == false)
                    return;

                UpdateLastEtag(instance);

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Node}) failed {failureInfo.RetriesCount} times, the connection will be retried on {failureInfo.RetryOn}.", e);

                _reconnectQueue.Add(failureInfo);
            }
        }

        private void UpdateLastEtag(OutgoingReplicationHandler instance)
        {
            var etagPerDestination = _lastSendEtagPerDestination.GetOrAdd(
                instance.Node,
                _ => new LastEtagPerDestination());

            if (etagPerDestination.LastEtag == instance._lastSentDocumentEtag)
                return;

            Interlocked.Exchange(ref etagPerDestination.LastEtag, instance._lastSentDocumentEtag);
        }

        private void OnOutgoingSendingSucceeded(OutgoingReplicationHandler instance)
        {
            UpdateLastEtag(instance);

            while (_waitForReplicationTasks.TryDequeue(out TaskCompletionSource<object> result))
            {
                TaskExecutor.Complete(result);
            }
        }

        private void ResetReplicationFailuresInfo(OutgoingReplicationHandler instance)
        {
            if (_outgoingFailureInfo.TryGetValue(instance.Node, out ConnectionShutdownInfo failureInfo))
                failureInfo.Reset();
        }

        private void OnIncomingReceiveSucceeded(IncomingReplicationHandler instance)
        {
            _incomingLastActivityTime.AddOrUpdate(instance.ConnectionInfo, DateTime.UtcNow, (_, __) => DateTime.UtcNow);

            // PERF: _incoming locks if you do _incoming.Values. Using .Select
            // directly and fetching the Value avoids this problem.
            foreach (var kv in _incoming)
            {
                var handler = kv.Value;
                if (handler != instance)
                    handler.OnReplicationFromAnotherSource();
            }
        }

        public void Dispose()
        {
            _locker.EnterWriteLock();

            try
            {
                var ea = new ExceptionAggregator("Failed during dispose of document replication loader");
                ea.Execute(() => _server.Cluster.Changes.DatabaseChanged -= DatabaseValueChanged);
                ea.Execute(() =>
                {
                    using (var waitHandle = new ManualResetEvent(false))
                    {
                        if (_reconnectAttemptTimer.Dispose(waitHandle))
                        {
                            waitHandle.WaitOne();
                        }
                    }
                });

                ea.Execute(() => ConflictResolver?.ResolveConflictsTask.Wait());

                ConflictResolver = null;

                if (_log.IsInfoEnabled)
                    _log.Info("Closing and disposing document replication connections.");

                foreach (var incoming in _incoming)
                    ea.Execute(incoming.Value.Dispose);

                foreach (var outgoing in _outgoing)
                    ea.Execute(outgoing.Dispose);

                Database.TombstoneCleaner?.Unsubscribe(this);

                Database = null;
                ea.ThrowIfNeeded();
            }
            finally
            {
                _locker.ExitWriteLock();
            }
        }

        public string TombstoneCleanerIdentifier => "Replication";

        public event Action<LiveReplicationPulsesCollector.ReplicationPulse> OutgoingReplicationConnectionFailed;

        public event Action<ReplicationNode, OutgoingReplicationFailureToConnectReporter> OutgoingReplicationConnectionErrored;

        public event Action<ReplicationNode, IncomingReplicationFailureToConnectReporter> IncomingReplicationConnectionErrored;

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            var minEtag = Math.Min(GetMinimalEtagForTombstoneCleanupWithHubReplication(), GetMinimalEtagForReplication());

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
            {
                { Constants.Documents.Collections.AllDocumentsCollection, minEtag },
                { Constants.TimeSeries.All, minEtag }
            };

            if (Destinations == null)
                return result;
            ReplicationNode disabledReplicationNode = null;
            bool hasDisabled = false;
            foreach (var replicationDocumentDestination in Destinations)
            {
                if (replicationDocumentDestination.Disabled)
                {
                    disabledReplicationNode = replicationDocumentDestination;
                    hasDisabled = true;
                    break;
                }
            }

            if (hasDisabled == false)
                return result;

            const int maxTombstones = 16 * 1024;

            bool tooManyTombstones;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                tooManyTombstones = Database.DocumentsStorage.HasMoreOfTombstonesAfter(context, minEtag, maxTombstones);
            }

            if (tooManyTombstones)
            {
                Database.NotificationCenter.Add(
                    PerformanceHint.Create(
                        database: Database.Name,
                        title: "Large number of tombstones because of disabled replication destination",
                        msg:
                        $"The disabled replication destination {disabledReplicationNode.FromString()} prevents from cleaning large number of tombstones.",

                        type: PerformanceHintType.Replication,
                        notificationSeverity: NotificationSeverity.Warning,
                        source: disabledReplicationNode.FromString()
                    ));
            }

            return result;
        }

        public class IncomingConnectionRejectionInfo
        {
            public string Reason { get; set; }
            public DateTime When { get; } = DateTime.UtcNow;
        }

        public class ConnectionShutdownInfo
        {
            private readonly TimeSpan _initialTimeout = TimeSpan.FromMilliseconds(1000);
            private readonly int _retriesCount = 0;

            public ConnectionShutdownInfo()
            {
                NextTimeout = _initialTimeout;
                RetriesCount = _retriesCount;
            }

            public string DestinationDbId;

            public bool External;

            public long LastHeartbeatTicks;

            public double MaxConnectionTimeout;

            public readonly Queue<Exception> Errors = new Queue<Exception>();

            public TimeSpan NextTimeout { get; set; }

            public DateTime RetryOn { get; set; }

            public ReplicationNode Node { get; set; }

            public int RetriesCount { get; set; }

            public void Reset()
            {
                NextTimeout = _initialTimeout;
                RetriesCount = _retriesCount;
                Errors.Clear();
            }

            public void OnError(Exception e)
            {
                Errors.Enqueue(e);
                while (Errors.Count > 25)
                    Errors.TryDequeue(out _);

                RetriesCount++;
                NextTimeout *= 2;
                NextTimeout = TimeSpan.FromMilliseconds(Math.Min(NextTimeout.TotalMilliseconds, MaxConnectionTimeout));
                RetryOn = DateTime.UtcNow + NextTimeout;
            }
        }

        public int GetSizeOfMajority()
        {
            return (_numberOfSiblings + 1) / 2 + 1;
        }

        public async Task<int> WaitForReplicationAsync(int numberOfReplicasToWaitFor, TimeSpan waitForReplicasTimeout, string lastChangeVector)
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var internalDestinations = _internalDestinations.Select(x => x.Url).ToHashSet();
                var waitForNextReplicationAsync = WaitForNextReplicationAsync();
                var past = ReplicatedPastInternalDestinations(internalDestinations, lastChangeVector);
                if (past >= numberOfReplicasToWaitFor)
                    return past;

                var remaining = waitForReplicasTimeout - sp.Elapsed;
                if (remaining < TimeSpan.Zero)
                    return ReplicatedPastInternalDestinations(internalDestinations, lastChangeVector);

                var timeout = TimeoutManager.WaitFor(remaining);
                try
                {
                    if (await Task.WhenAny(waitForNextReplicationAsync, timeout) == timeout)
                        return ReplicatedPastInternalDestinations(internalDestinations, lastChangeVector);
                }
                catch (OperationCanceledException e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Get exception while trying to get write assurance on a database with {numberOfReplicasToWaitFor} servers. " +
                                  $"Written so far to {past} servers only. " +
                                  $"LastChangeVector is: {lastChangeVector}.", e);
                    return ReplicatedPastInternalDestinations(internalDestinations, lastChangeVector);
                }
            }
        }

        private Task WaitForNextReplicationAsync()
        {
            if (_waitForReplicationTasks.TryPeek(out TaskCompletionSource<object> result))
                return result.Task;

            result = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waitForReplicationTasks.Enqueue(result);
            return result.Task;
        }

        private int ReplicatedPast(string changeVector)
        {
            var count = 0;
            foreach (var destination in _outgoing)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(changeVector, destination.LastAcceptedChangeVector);
                if (conflictStatus == ConflictStatus.AlreadyMerged)
                    count++;
            }
            return count;
        }

        private int ReplicatedPastInternalDestinations(HashSet<string> internalUrls, string changeVector)
        {
            var count = 0;
            foreach (var destination in _outgoing)
            {
                if (internalUrls.Contains(destination.Destination.Url) == false)
                    continue;

                var conflictStatus = ChangeVectorUtils.GetConflictStatus(changeVector, destination.LastAcceptedChangeVector);
                if (conflictStatus == ConflictStatus.AlreadyMerged)
                    count++;
            }
            return count;
        }

        public int GetNextReplicationStatsId()
        {
            return Interlocked.Increment(ref _replicationStatsId);
        }
    }

    public class OutgoingReplicationFailureToConnectReporter : IReportOutgoingReplicationPerformance
    {
        private ReplicationNode _node;
        private OutgoingReplicationStatsAggregator _stats;

        public OutgoingReplicationFailureToConnectReporter(ReplicationNode node, OutgoingReplicationStatsAggregator stats)
        {
            _node = node;
            _stats = stats;
        }

        public string DestinationFormatted => $"{_node.Url}/databases/{_node.Database}";

        public OutgoingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            return new[] { _stats.ToReplicationPerformanceStats() };
        }
    }

    public class IncomingReplicationFailureToConnectReporter : IReportIncomingReplicationPerformance
    {
        private ReplicationNode _node;
        private IncomingReplicationStatsAggregator _stats;

        public IncomingReplicationFailureToConnectReporter(ReplicationNode node, IncomingReplicationStatsAggregator stats)
        {
            _node = node;
            _stats = stats;
        }

        public string DestinationFormatted => $"{_node.Url}/databases/{_node.Database}";

        public IncomingReplicationPerformanceStats[] GetReplicationPerformance()
        {
            return new[] { _stats.ToReplicationPerformanceStats() };
        }
    }
}
