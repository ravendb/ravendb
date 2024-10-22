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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationLoader : AbstractReplicationLoader<DocumentsContextPool, DocumentsOperationContext>, ITombstoneAware
    {
        private readonly Timer _reconnectAttemptTimer;
        private long _reconnectInProgress;
        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public event Action<IncomingReplicationHandler> IncomingReplicationAdded;

        public event Action<IncomingReplicationHandler> IncomingReplicationRemoved;

        public event Action<DatabaseOutgoingReplicationHandler> OutgoingReplicationAdded;

        public event Action<DatabaseOutgoingReplicationHandler> OutgoingReplicationRemoved;

        internal ManualResetEventSlim DebugWaitAndRunReplicationOnce;
        internal readonly int MinimalHeartbeatInterval;

        public DocumentDatabase Database;
        private SingleUseFlag _isInitialized = new SingleUseFlag();

        public ResolveConflictOnReplicationConfigurationChange ConflictResolver;

        private readonly ConcurrentSet<DatabaseOutgoingReplicationHandler> _outgoing = new ConcurrentSet<DatabaseOutgoingReplicationHandler>();
        private readonly ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo> _outgoingFailureInfo = new ConcurrentDictionary<ReplicationNode, ConnectionShutdownInfo>();
        private readonly ConcurrentSet<ConnectionShutdownInfo> _reconnectQueue = new ConcurrentSet<ConnectionShutdownInfo>();
        private readonly ConcurrentDictionary<IncomingConnectionInfo, DateTime> _incomingLastActivityTime = new ConcurrentDictionary<IncomingConnectionInfo, DateTime>();
        internal readonly ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> _incomingRejectionStats = new ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>();
        private readonly ConcurrentBag<ReplicationNode> _internalDestinations = new ConcurrentBag<ReplicationNode>();
        private readonly HashSet<ExternalReplicationBase> _externalDestinations = new HashSet<ExternalReplicationBase>();
        private List<ReplicationNode> _destinations = new List<ReplicationNode>();
        protected ClusterTopology _clusterTopology = new ClusterTopology();
        private int _numberOfSiblings;
        public ConflictSolver ConflictSolverConfig;
        private readonly CancellationToken _shutdownToken;
        private HubInfoForCleaner _hubInfoForCleaner;
        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks = new ConcurrentQueue<TaskCompletionSource<object>>();
        private readonly ConcurrentDictionary<ReplicationNode, LastEtagPerDestination> _lastSendEtagPerDestination = new ConcurrentDictionary<ReplicationNode, LastEtagPerDestination>();

        public IEnumerable<ReplicationNode> OutgoingConnections => _outgoing.Select(x => x.Node);
        public IEnumerable<DatabaseOutgoingReplicationHandler> OutgoingHandlers => _outgoing;
        public IEnumerable<ReplicationNode> ReconnectQueue => _reconnectQueue.Select(x => x.Node);
        public IReadOnlyDictionary<ReplicationNode, ConnectionShutdownInfo> OutgoingFailureInfo => _outgoingFailureInfo;

        public event Action<IncomingReplicationHandler, int> AttachmentStreamsReceived;
        public IReadOnlyDictionary<IncomingConnectionInfo, DateTime> IncomingLastActivityTime => _incomingLastActivityTime;
        public IReadOnlyDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> IncomingRejectionStats => _incomingRejectionStats;
        public List<ReplicationNode> Destinations => _destinations;

        private sealed class HubInfoForCleaner
        {
            public long LastEtag;
            public DateTime LastCleanup;
        }

        private sealed class LastEtagPerDestination
        {
            public long LastEtag;
        }

        public ReplicationLoader(DocumentDatabase database, ServerStore server) : base(server, database.Name, database.DocumentsStorage.ContextPool, database.DatabaseShutdown)
        {
            Database = database;
            _shutdownToken = database.DatabaseShutdown;
            database.TombstoneCleaner.Subscribe(this);
            server.Cluster.Changes.DatabaseChanged += DatabaseValueChanged;
            var config = database.Configuration.Replication;
            var reconnectTime = config.RetryReplicateAfter.AsTimeSpan;
            _reconnectAttemptTimer = new Timer(state => ForceTryReconnectAll(),
                null, reconnectTime, reconnectTime);
            MinimalHeartbeatInterval = (int)config.ReplicationMinimalHeartbeat.AsTimeSpan.TotalMilliseconds;
        }

        public long GetMinimalEtagForReplication()
        {
            DatabaseTopology topology;
            long minEtag = long.MaxValue;

            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var dbRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name);
                topology = dbRecord.Topology;
                var externals = dbRecord.ExternalReplications;
                if (externals != null)
                {
                    foreach (var external in externals)
                    {
                        var state = GetExternalReplicationState(_server, Database.Name, external.TaskId, ctx);
                        var myEtag = ChangeVectorUtils.GetEtagById(state.SourceChangeVector, Database.DbBase64Id);
                        minEtag = Math.Min(myEtag, minEtag);
                    }
                }
            }
            var replicationNodes = new List<ReplicationNode>();

            foreach (var node in topology.AllNodes)
            {
                if (node == _server.NodeTag)
                    continue;
                var internalReplication = new InternalReplication
                {
                    NodeTag = node,
                    Url = _clusterTopology.GetUrlFromTag(node),
                    Database = Database.Name
                };
                replicationNodes.Add(internalReplication);
            }

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
                    Interlocked.Exchange(ref _hubInfoForCleaner, new HubInfoForCleaner { LastCleanup = Database.Time.GetUtcNow(), LastEtag = max });
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
                var oldEtag = -1L;

                while (true)
                {
                    var newEtag = (max + min) / 2;
                    if (newEtag == oldEtag)
                    {
                        Interlocked.Exchange(ref _hubInfoForCleaner, new HubInfoForCleaner { LastCleanup = Database.Time.GetUtcNow(), LastEtag = min });
                        return min;
                    }

                    oldEtag = newEtag;
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
                    DisposeRelatedPullReplication(reg.HubName, reg.CertificateThumbprint, reg.Database);
                    break;
            }
            return Task.CompletedTask;

            void DisposeRelatedPullReplication(string hub, string certThumbprint, string sourceDatabase = null)
            {
                if (hub == null)
                    return;

                foreach (var (key, repl) in _incoming)
                {
                    if (repl is IncomingPullReplicationHandler pullHandler == false)
                        continue;

                    if (string.Equals(pullHandler._incomingPullReplicationParams.Name, hub, StringComparison.OrdinalIgnoreCase) == false ||
                        (string.IsNullOrEmpty(sourceDatabase) == false &&
                         string.Equals(pullHandler._incomingPullReplicationParams.SourceDatabaseName, sourceDatabase, StringComparison.OrdinalIgnoreCase) == false))
                        continue;

                    if (certThumbprint != null && pullHandler.CertificateThumbprint != certThumbprint)
                        continue;

                    try
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Resetting {repl.ConnectionInfo} for {hub} on {certThumbprint} because replication configuration changed. Will be reconnected.");
                        repl.Dispose();
                        _incoming.TryRemove(key, out _);
                    }
                    catch
                    {
                    }
                }

                foreach (var repl in _outgoing)
                {
                    if (repl is OutgoingPullReplicationHandlerAsHub asHub == false)
                        continue;

                    if (string.Equals(asHub.PullReplicationDefinitionName, hub, StringComparison.OrdinalIgnoreCase) == false ||
                        (string.IsNullOrEmpty(sourceDatabase) == false &&
                         string.Equals(sourceDatabase, repl.Destination.Database, StringComparison.OrdinalIgnoreCase) == false))
                        continue;

                    if (certThumbprint != null && asHub.CertificateThumbprint != certThumbprint)
                        continue;

                    try
                    {
                        repl.Dispose();
                        _outgoing.TryRemove(repl);
                    }
                    catch
                    {
                    }
                }
            }
        }

        public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions,
            TcpConnectionHeaderMessage header,
            X509Certificate2 certificate,
            JsonOperationContext.MemoryBuffer buffer)
        {
            var supportedVersions = GetSupportedVersions(tcpConnectionOptions);
            var initialRequest = GetReplicationInitialRequest(tcpConnectionOptions, supportedVersions, buffer);

            string[] allowedPaths = default;
            string pullDefinitionName = null;
            PreventDeletionsMode preventDeletionsMode = PreventDeletionsMode.None;
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

                            allowedPaths = DetailedReplicationHubAccess.Preferred(header.ReplicationHubAccess.AllowedSinkToHubPaths, header.ReplicationHubAccess.AllowedHubToSinkPaths);
                            preventDeletionsMode = pullReplicationDefinition.PreventDeletionsMode;

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

            PullReplicationParams pullReplicationParams = null;
            if (pullDefinitionName != null)
            {
                pullReplicationParams = new PullReplicationParams()
                {
                    Name = pullDefinitionName,
                    AllowedPaths = allowedPaths,
                    Mode = PullReplicationMode.SinkToHub,
                    PreventDeletionsMode = preventDeletionsMode,
                    Type = PullReplicationParams.ConnectionType.Incoming
                };
            }

            CreateIncomingInstance(tcpConnectionOptions, buffer, pullReplicationParams);
        }

        private void CreatePullReplicationAsHub(TcpConnectionOptions tcpConnectionOptions, ReplicationInitialRequest initialRequest,
                        TcpConnectionHeaderMessage.SupportedFeatures supportedVersions,
                        PullReplicationDefinition pullReplicationDefinition, TcpConnectionHeaderMessage header)
        {
            if (string.Equals(initialRequest.PullReplicationDefinitionName, pullReplicationDefinition.Name, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException(
                    $"PullReplicationDefinitionName '{initialRequest.PullReplicationDefinitionName}' does not match the pull replication definition name: {pullReplicationDefinition.Name}");

            var taskId = pullReplicationDefinition.TaskId; // every connection to this pull replication on the hub will have the same task id.
            var externalReplication = pullReplicationDefinition.ToPullReplicationAsHub(initialRequest, taskId);

            var outgoingReplication = new OutgoingPullReplicationHandlerAsHub(this, Database, externalReplication, initialRequest.Info)
            {
                OutgoingPullReplicationParams = new PullReplicationParams
                {
                    Name = initialRequest.PullReplicationDefinitionName,
                    PreventDeletionsMode = pullReplicationDefinition.PreventDeletionsMode,
                    Mode = pullReplicationDefinition.Mode,
                    Type = PullReplicationParams.ConnectionType.Outgoing
                },

                PullReplicationDefinitionName = initialRequest.PullReplicationDefinitionName,
                CertificateThumbprint = tcpConnectionOptions.Certificate?.Thumbprint
            };

            if (header.ReplicationHubAccess != null)
            {
                // Note that if the certificate isn't registered *specifically* in the pull replication, we don't do
                // any filtering. That means that the certificate has global access to the database, so there is not point
                outgoingReplication.PathsToSend = DetailedReplicationHubAccess.Preferred(header.ReplicationHubAccess.AllowedHubToSinkPaths, header.ReplicationHubAccess.AllowedSinkToHubPaths);
            }

            if (_outgoing.TryAdd(outgoingReplication) == false)
            {
                using (tcpConnectionOptions)
                using (outgoingReplication)
                {

                }
                return;
            }

            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            outgoingReplication.SuccessfulReplication += ResetReplicationFailuresInfo;

            outgoingReplication.StartPullReplicationAsHub(tcpConnectionOptions.Stream, supportedVersions);
            OutgoingReplicationAdded?.Invoke(outgoingReplication);
        }

        public void RunPullReplicationAsSink(
            TcpConnectionOptions tcpConnectionOptions,
            JsonOperationContext.MemoryBuffer buffer,
            PullReplicationAsSink destination,
            DatabaseOutgoingReplicationHandler source)
        {
            using (source)
            {
                string[] allowedPaths = DetailedReplicationHubAccess.Preferred(destination.AllowedHubToSinkPaths, destination.AllowedSinkToHubPaths);
                var incomingPullParams = new PullReplicationParams
                {
                    Name = destination.HubName,
                    AllowedPaths = allowedPaths,
                    Mode = PullReplicationMode.HubToSink,
                    PreventDeletionsMode = null,
                    Type = PullReplicationParams.ConnectionType.Incoming
                };
                var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, incomingPullParams);
                newIncoming.Failed += RetryPullReplication;

                _outgoing.TryRemove(source); // we are pulling and therefore incoming, upon failure 'RetryPullReplication' will put us back as an outgoing

                PoolOfThreads.PooledThread.ResetCurrentThreadName();
                Thread.CurrentThread.Name = ThreadNames.GetNameToUse(ThreadNames.ForPullReplicationAsSink($"Pull Replication as Sink from {destination.Database} at {destination.Url}", destination.Database, destination.Url));

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
                        instance.AttachmentStreamsReceived -= OnAttachmentStreamsReceived;
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Pull replication Sink handler has thrown an unhandled exception. ({instance.FromToString})", e);
                    }

                    // if the stream closed, it is our duty to reconnect
                    AddAndStartOutgoingReplication(destination);
                }
            }
        }

        private void OnAttachmentStreamsReceived(IncomingReplicationHandler source, int attachmentsStreamCount)
        {
            AttachmentStreamsReceived?.Invoke(source, attachmentsStreamCount);
        }

        private void CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer, PullReplicationParams pullReplicationParams)
        {
            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, pullReplicationParams);
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
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("you can't add two identical connections.", new InvalidOperationException("you can't add two identical connections."));
                }
                newIncoming.Dispose();
            }
        }

        internal void AddAndStartOutgoingReplication(ReplicationNode node)
        {
            var info = GetConnectionInfo(node);

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
                DatabaseOutgoingReplicationHandler outgoingReplication = GetOutgoingReplicationHandlerInstance(info, node);

                if (outgoingReplication == null)
                    return;

                if (_outgoing.TryAdd(outgoingReplication) == false)
                {
                    outgoingReplication.Dispose();
                    return;
                }

                outgoingReplication.Failed += OnOutgoingSendingFailed;
                outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
                outgoingReplication.SuccessfulReplication += ResetReplicationFailuresInfo;

                OutgoingReplicationAdded?.Invoke(outgoingReplication);

                outgoingReplication.Start();
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public sealed class PullReplicationParams
        {
            public string Name;
            public string SourceDatabaseName;
            public string[] AllowedPaths;
            public PullReplicationMode Mode;
            public PreventDeletionsMode? PreventDeletionsMode;
            public ConnectionType Type;

            public enum ConnectionType
            {
                None,
                Incoming,
                Outgoing
            }
        }

        private IncomingReplicationHandler CreateIncomingReplicationHandler(
            TcpConnectionOptions tcpConnectionOptions,
            JsonOperationContext.MemoryBuffer buffer,
            PullReplicationParams incomingPullParams)
        {
            var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, buffer, incomingPullParams);

            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer, incomingPullParams, getLatestEtagMessage);

            newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
            newIncoming.AttachmentStreamsReceived += OnAttachmentStreamsReceived;

            return newIncoming;
        }

        protected virtual IncomingReplicationHandler CreateIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer,
            PullReplicationParams incomingPullParams, ReplicationLatestEtagRequest getLatestEtagMessage)
        {
            if (incomingPullParams == null)
            {
                return new IncomingReplicationHandler(
                    tcpConnectionOptions,
                    getLatestEtagMessage,
                    this,
                    buffer,
                    getLatestEtagMessage.ReplicationsType);
            }

            return new IncomingPullReplicationHandler(
                tcpConnectionOptions,
                getLatestEtagMessage,
                this,
                buffer,
                getLatestEtagMessage.ReplicationsType,
                incomingPullParams);
        }

        internal static readonly TimeSpan MaxInactiveTime = TimeSpan.FromSeconds(60);

        protected override DynamicJsonValue GetInitialRequestMessage(ReplicationLatestEtagRequest getLatestEtagMessage, PullReplicationParams replParams = null)
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (Database.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationContext))
            using (documentsContext.OpenReadTransaction())
            using (configurationContext.OpenReadTransaction())
            {
                string changeVector = null;
                long lastEtagFromSrc = 0;

                if (getLatestEtagMessage.ReplicationsType != ReplicationLatestEtagRequest.ReplicationType.Migration)
                {
                    changeVector = DocumentsStorage.GetFullDatabaseChangeVector(documentsContext);

                    lastEtagFromSrc = DocumentsStorage.GetLastReplicatedEtagFrom(
                        documentsContext, getLatestEtagMessage.SourceDatabaseId);
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"GetLastEtag response, last etag: {lastEtagFromSrc}");
                }

                var response = base.GetInitialRequestMessage(getLatestEtagMessage, replParams);
                response[nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtagFromSrc;
                response[nameof(ReplicationMessageReply.DatabaseChangeVector)] = changeVector;

                return response;
            }
        }

        protected override void AssertValidConnection(IncomingConnectionInfo connectionInfo)
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

            base.AssertValidConnection(connectionInfo);

            if (_incoming.TryGetValue(connectionInfo.SourceDatabaseId, out var value))
            {
                if (value is IncomingReplicationHandler incoming == false)
                    throw new InvalidOperationException(
                        $"An active connection for this database already exists from {value.ConnectionInfo.SourceUrl} or is not from type 'IncomingReplicationHandler'.");

                var lastHeartbeat = new DateTime(incoming.LastHeartbeatTicks);
                if (lastHeartbeat + MaxInactiveTime > Database.Time.GetUtcNow())
                    throw new InvalidOperationException(
                        $"An active connection for this database already exists from {value.ConnectionInfo.SourceUrl} (last heartbeat: {lastHeartbeat}).");

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Disconnecting existing connection from {incoming.FromToString} because we got a new connection from the same source db " +
                                 $"(last heartbeat was at {lastHeartbeat}).");

                IncomingReplicationRemoved?.Invoke(incoming);

                value.Dispose();
            }
        }

        public void Initialize(DatabaseRecord record, long index)
        {
            if (_isInitialized) //precaution -> probably not necessary, but still...
                return;

            ConflictSolverConfig = record.ConflictSolverConfig;
            ConflictResolver = new ResolveConflictOnReplicationConfigurationChange(this, _logger);
            Task.Run(() => ConflictResolver.RunConflictResolversOnce(record.ConflictSolverConfig, index));
            _isInitialized.Raise();
        }

        public void HandleDatabaseRecordChange(DatabaseRecord newRecord, long index)
        {
            HandleConflictResolverChange(newRecord, index);
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
                    if (ValidateConnectionString(newRecord.RavenConnectionStrings, external, out var connectionString))
                    {
                        external.ConnectionString = connectionString;
                    }
                }
            }
        }

        private void HandleConflictResolverChange(DatabaseRecord newRecord, long index)
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
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Conflict resolution was change.");
                }
                ConflictSolverConfig = newRecord.ConflictSolverConfig;
                Task.Run(() => ConflictResolver.RunConflictResolversOnce(newRecord.ConflictSolverConfig, index));
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

            HandleReplicationChanges(newRecord, instancesToDispose);

            var destinations = new List<ReplicationNode>();
            destinations.AddRange(_internalDestinations);
            destinations.AddRange(_externalDestinations);
            _destinations = destinations;
            _numberOfSiblings = _destinations.Select(x => x.Url).Intersect(_clusterTopology.AllNodes.Select(x => x.Value)).Count();

            DisposeConnections(instancesToDispose);
        }

        protected virtual void HandleReplicationChanges(DatabaseRecord newRecord, List<IDisposable> instancesToDispose)
        {
            HandleInternalReplication(newRecord, instancesToDispose);
            HandleExternalReplication(newRecord, instancesToDispose);
            HandleHubPullReplication(newRecord, instancesToDispose);
        }

        private void HandleHubPullReplication(DatabaseRecord newRecord, List<IDisposable> instancesToDispose)
        {
            foreach (var instance in OutgoingHandlers)
            {
                if (instance is OutgoingPullReplicationHandlerAsHub asHub == false)
                    continue;

                var pullReplication = newRecord.HubPullReplications.Find(x => x.Name == asHub.PullReplicationDefinitionName);

                if (pullReplication != null && pullReplication.Disabled == false && Database.DisableOngoingTasks == false)
                {
                    // update the destination
                    var current = (ExternalReplication)instance.Destination;
                    if (current.DelayReplicationFor != pullReplication.DelayReplicationFor)
                    {
                        current.DelayReplicationFor = pullReplication.DelayReplicationFor;
                        instance.NextReplicateTicks = 0;
                    }
                    current.MentorNode = pullReplication.MentorNode;
                    continue;
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Stopping replication to {instance.Destination.FromString()}");

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

        private void ForceTryReconnectAll()
        {
            if (_reconnectQueue.Count == 0)
                return;

            if (Interlocked.CompareExchange(ref _reconnectInProgress, 1, 0) == 1)
                return;

            try
            {
                DatabaseTopology topology;
                Dictionary<string, RavenConnectionString> ravenConnectionStrings;

                using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var raw = _server.Cluster.ReadRawDatabaseRecord(ctx, Database.Name);
                    if (raw == null)
                    {
                        _reconnectQueue.Clear();
                        return;
                    }

                    topology = raw.Topology;
                    ravenConnectionStrings = raw.RavenConnectionStrings;
                }

                var cts = GetCancellationToken();
                foreach (var failure in _reconnectQueue)
                {
                    if (cts.IsCancellationRequested)
                        return;

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

                        if (failure.Node is ExternalReplicationBase exNode &&
                            IsMyTask(ravenConnectionStrings, topology, exNode) == false)
                            // no longer my task
                            continue;

                        if (failure.Node is BucketMigrationReplication migration)
                        {
                            if (topology.WhoseTaskIsIt(RachisState.Follower, migration.ShardBucketMigration, getLastResponsibleNode: null) != _server.NodeTag)
                                // no longer my task
                                continue;

                            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {
                                var raw = _server.Cluster.ReadRawDatabaseRecord(ctx, ShardHelper.ToDatabaseName(Database.Name));
                                if (raw.Sharding.BucketMigrations.TryGetValue(migration.Bucket, out var current) == false)
                                    continue;

                                if (migration.ForBucketMigration(current) == false)
                                    continue;
                            }
                        }

                        AddAndStartOutgoingReplication(failure.Node);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"Failed to start outgoing replication to {failure.Node}", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations("Unexpected exception during ForceTryReconnectAll", e);
                }
            }
            finally
            {
                Interlocked.Exchange(ref _reconnectInProgress, 0);
            }
        }

        private bool IsMyTask(Dictionary<string, RavenConnectionString> connectionStrings, DatabaseTopology topology, ExternalReplicationBase task)
        {
            if (ValidateConnectionString(connectionStrings, task, out _) == false)
                return false;

            var taskStatus = GetExternalReplicationState(_server, Database.Name, task.TaskId);
            var whoseTaskIsIt = OngoingTasksUtils.WhoseTaskIsIt(Server, topology, task, taskStatus, Database.NotificationCenter);
            return whoseTaskIsIt == _server.NodeTag;
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

        private void DropIncomingConnections(IEnumerable<ReplicationNode> connectionsToRemove, List<IDisposable> instancesToDispose)
        {
            var toRemove = connectionsToRemove?.ToList();
            if (toRemove == null || toRemove.Count == 0)
                return;

            // this is relevant for sink
            foreach (var incoming in _incoming)
            {
                var instance = incoming.Value as IncomingReplicationHandler;
                if (toRemove.Any(conn => conn.Url == incoming.Value.ConnectionInfo.SourceUrl))
                {
                    if (_incoming.TryRemove(incoming.Value.ConnectionInfo.SourceDatabaseId, out _))
                        IncomingReplicationRemoved?.Invoke(instance);
                    instance?.ClearEvents();
                    instancesToDispose.Add(incoming.Value);
                }
            }
        }

        private void DisposeConnections(List<IDisposable> instancesToDispose)
        {
            ThreadPool.QueueUserWorkItem(toDispose =>
            {
                Parallel.ForEach((List<IDisposable>)toDispose, instance =>
                {
                    try
                    {
                        instance?.Dispose();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            switch (instance)
                            {
                                case DatabaseOutgoingReplicationHandler outHandler:
                                    _logger.Info($"Failed to dispose outgoing replication to {outHandler.DestinationFormatted}", e);
                                    break;

                                case IncomingReplicationHandler inHandler:
                                    _logger.Info($"Failed to dispose incoming replication to {inHandler.SourceFormatted}", e);
                                    break;

                                default:
                                    _logger.Info($"Failed to dispose an unknown type '{instance?.GetType().FullName}", e);
                                    break;
                            }
                        }
                    }
                });
            }, instancesToDispose);
        }

        private (List<ExternalReplicationBase> AddedDestinations, List<ExternalReplicationBase> RemovedDestiantions) FindExternalReplicationChanges(
            DatabaseRecord databaseRecord, HashSet<ExternalReplicationBase> current,
            List<ExternalReplicationBase> newDestinations)
        {
            var outgoingHandlers = OutgoingHandlers.ToList();

            var addedDestinations = new List<ExternalReplicationBase>();
            var removedDestinations = current.ToList();
            foreach (var newDestination in newDestinations.ToArray())
            {
                if (IsMyTask(databaseRecord.RavenConnectionStrings, databaseRecord.Topology, newDestination) == false)
                    continue;

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

                    if (handler.Destination is ExternalReplicationBase erb)
                    {
                        erb.MentorNode = newDestination.MentorNode;

                        if (handler.Destination is ExternalReplication ex &&
                            actual is ExternalReplication actualEx &&
                            newDestination is ExternalReplication newDestinationEx)
                        {
                            if (ex.DelayReplicationFor != actualEx.DelayReplicationFor)
                                handler.NextReplicateTicks = 0;

                            ex.DelayReplicationFor = newDestinationEx.DelayReplicationFor;
                        }
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

            var changes = FindExternalReplicationChanges(newRecord, _externalDestinations, externalReplications);

            DropOutgoingConnections(changes.RemovedDestiantions, instancesToDispose);
            DropIncomingConnections(changes.RemovedDestiantions, instancesToDispose);

            var newDestinations = GetMyNewDestinations(newRecord, changes.AddedDestinations);

            if (newDestinations.Count > 0 && Database.DisableOngoingTasks == false)
            {
                Task.Run(() =>
                {
                    // here we might have blocking calls to fetch the tcp info.
                    try
                    {
                        StartOutgoingConnections(newDestinations);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to start the outgoing connections to {newDestinations.Count} new destinations", e);
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
                if (ValidateConnectionString(newRecord.RavenConnectionStrings, externalReplication, out var connectionString) == false)
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
                        CertificateWithPrivateKey = sink.CertificateWithPrivateKey,
                        AccessName = sink.AccessName
                    };

                    i += 1;
                    externalReplications.Insert(i, other);
                }
            }
        }

        private List<ExternalReplicationBase> GetMyNewDestinations(DatabaseRecord newRecord, List<ExternalReplicationBase> added)
        {
            return added.Where(configuration => IsMyTask(newRecord.RavenConnectionStrings, newRecord.Topology, configuration)).ToList();
        }

        protected override CancellationToken GetCancellationToken() => Database.DatabaseShutdown;

        public void CompleteDeletionIfNeeded(CancellationTokenSource cts)
        {
            var dbName = Database.Name;
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = _server.Cluster.ReadRawDatabaseRecord(ctx, dbName))
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
                    ThreadPool.QueueUserWorkItem(_ =>
                        {
                            try
                            {
                                _server.DatabasesLandlord.DeleteIfNeeded(dbName, fromReplication: true);
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsOperationsEnabled)
                                {
                                    _logger.Operations("Unexpected error during database deletion from replication loader", e);
                                }
                            }
                        }
                        , null);
                }
            }
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
                }).ToList();

                DropOutgoingConnections(removed, instancesToDispose);
                DropIncomingConnections(removed, instancesToDispose);
            }

            if (internalConnections.AddedDestinations.Count > 0)
            {
                var added = internalConnections.AddedDestinations.Select(r => new InternalReplication
                {
                    NodeTag = _clusterTopology.TryGetNodeTagByUrl(r).NodeTag,
                    Url = r,
                    Database = Database.Name
                }).ToList();

                _ = Task.Run(() =>
                {
                    // here we might have blocking calls to fetch the tcp info.
                    try
                    {
                        StartOutgoingConnections(added);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to start the outgoing connections to {added.Count} new destinations", e);
                    }
                });
            }

            _internalDestinations.Clear();

            if (newInternalDestinations != null)
            {
                foreach (var item in newInternalDestinations)
                {
                    _internalDestinations.Add(item);
                }
            }
        }

        private void StartOutgoingConnections(IReadOnlyCollection<ReplicationNode> connectionsToAdd)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Initializing {connectionsToAdd.Count:#,#} outgoing replications from {Database} on {_server.NodeTag}.");

            foreach (var destination in connectionsToAdd)
            {
                if (destination.Disabled)
                    continue;

                if (_logger.IsInfoEnabled)
                    _logger.Info("Initialized outgoing replication for " + destination.FromString());
                AddAndStartOutgoingReplication(destination);
            }

            if (_logger.IsInfoEnabled)
                _logger.Info("Finished initialization of outgoing replications..");
        }

        protected void DropOutgoingConnections(IEnumerable<ReplicationNode> connectionsToRemove, List<IDisposable> instancesToDispose)
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

            if (_logger.IsInfoEnabled)
                _logger.Info($"Dropping {outgoingChanged.Count:#,#} outgoing replications connections from {Database} on {_server.NodeTag}.");

            foreach (var instance in outgoingChanged)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Stopping replication to {instance.Destination.FromString()}");

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

        private TcpConnectionInfo GetConnectionInfo(ReplicationNode node)
        {
            var shutdownInfo = _outgoingFailureInfo.GetOrAdd(node, new ConnectionShutdownInfo
            {
                Node = node,
                MaxConnectionTimeout = Database.Configuration.Replication.RetryMaxTimeout.AsTimeSpan.TotalMilliseconds
            });

            X509Certificate2 certificate = null;
            try
            {
                certificate = GetCertificateForReplication(node, out _);

                switch (node)
                {
                    case PullReplicationAsSink sinkNode:
                        return GetPullReplicationTcpInfo(sinkNode, certificate, sinkNode.ConnectionString.Database);

                    case ExternalReplication exNode:
                        string database = exNode.ConnectionString.Database;
                        return GetExternalReplicationTcpInfo(exNode, certificate, database);

                    case BucketMigrationReplication _:
                    case InternalReplication _:
                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownToken))
                        {
                            cts.CancelAfter(_server.Engine.TcpConnectionTimeout);
                            return ReplicationUtils.GetTcpInfoForInternalReplication(node.Url, node.Database, Database.DbId.ToString(),
                                Database.ReadLastEtag(),
                                "Replication",
                                certificate, _server.NodeTag, cts.Token);
                        }
                    default:
                        throw new InvalidOperationException(
                            $"Unexpected replication node type, Expected to be '{typeof(ExternalReplication)}' or '{typeof(InternalReplication)}', but got '{node.GetType()}'");
                }
            }
            catch (Exception e)
            {
                if (_shutdownToken.IsCancellationRequested)
                    return null;

                // will try to fetch it again later
                if (_logger.IsInfoEnabled)
                {
                    if (e is DatabaseIdleException)
                    {
                        // this is expected, so we don't mark it as error
                        _logger.Info($"The database is idle on the destination '{node.FromString()}', the connection will be retried later.");
                    }
                    else
                    {
                        _logger.Info($"Failed to fetch tcp connection information for the destination '{node.FromString()}' , the connection will be retried later.", e);
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
                    IsExternal = node is ExternalReplicationBase,
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

        protected virtual DatabaseOutgoingReplicationHandler GetOutgoingReplicationHandlerInstance(TcpConnectionInfo info, ReplicationNode node)
        {
            if (Database == null)
                return null;

            DatabaseOutgoingReplicationHandler outgoingReplication;

            switch (node)
            {
                case PullReplicationAsSink sinkNode:
                    outgoingReplication = new OutgoingPullReplicationHandlerAsSink(this, Database, sinkNode, info);
                    break;
                case InternalReplication clusterNode:
                    outgoingReplication = new OutgoingInternalReplicationHandler(this, Database, clusterNode, info);
                    break;
                case ExternalReplication externalNode:
                    outgoingReplication = new OutgoingExternalReplicationHandler(this, Database, externalNode, info);
                    break;
                default:
                    throw new ArgumentException($"Unknown node type {node.GetType().FullName}");
            }

            return outgoingReplication;
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
                // use short term request executor that doesn't execute FirstTopologyUpdate because we do not have the authentication for that at this point
                using (var requestExecutor = RequestExecutor.CreateForShortTermUse(pullReplicationAsSink.ConnectionString.TopologyDiscoveryUrls, pullReplicationAsSink.ConnectionString.Database,
                    certificate, DocumentConventions.DefaultForServer))
                {
                    var cmd = new GetRemoteTaskTopologyCommand(database, Database.DatabaseGroupId, remoteTask);

                    try
                    {
                        requestExecutor.ExecuteWithCancellationToken(cmd, ctx, _shutdownToken);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Failed to execute {nameof(GetRemoteTaskTopologyCommand)} for {pullReplicationAsSink.Name}", e);

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
                using (var requestExecutor = RequestExecutor.CreateForShortTermUse(remoteDatabaseUrls,
                    pullReplicationAsSink.ConnectionString.Database, certificate, DocumentConventions.DefaultForServer))
                {
                    var cmd = new GetTcpInfoForRemoteTaskCommand(ExternalReplicationTag, database, remoteTask);

                    try
                    {
                        requestExecutor.ExecuteWithCancellationToken(cmd, ctx, _shutdownToken);
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
            using (var requestExecutor = RequestExecutor.CreateForServer(exNode.ConnectionString.TopologyDiscoveryUrls, exNode.ConnectionString.Database, certificate, DocumentConventions.DefaultForServer))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var cmd = new GetTcpInfoCommand(_server.GetNodeHttpServerUrl(), ExternalReplicationTag, database, Database.DbId.ToString(), Database.ReadLastEtag());
                try
                {
                    requestExecutor.ExecuteWithCancellationToken(cmd, ctx, _shutdownToken);
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

        public (string Url, OngoingTaskConnectionStatus Status) GetExternalReplicationDestination(long taskId, out string fromToString)
        {
            foreach (var outgoingHandler in OutgoingHandlers)
            {
                if (outgoingHandler.Destination is ExternalReplication ex && ex.TaskId == taskId)
                {
                    fromToString = outgoingHandler.FromToString;
                    return (ex.Url, OngoingTaskConnectionStatus.Active);
                }
            }

            fromToString = string.Empty;
            foreach (var reconnect in ReconnectQueue)
            {
                if (reconnect is ExternalReplication ex && ex.TaskId == taskId)
                    return (ex.Url, OngoingTaskConnectionStatus.Reconnect);
            }
            return (null, OngoingTaskConnectionStatus.NotActive);
        }

        public (string Url, OngoingTaskConnectionStatus Status) GetPullReplicationDestination(long taskId, string db)
        {
            //outgoing connections have the same task id per pull replication
            foreach (var outgoing in OutgoingConnections)
            {
                if (outgoing is ExternalReplication ex && ex.TaskId == taskId && db.Equals(outgoing.Database, StringComparison.OrdinalIgnoreCase))
                    return (ex.Url, OngoingTaskConnectionStatus.Active);
            }
            foreach (var reconnect in ReconnectQueue)
            {
                if (reconnect is ExternalReplication ex && ex.TaskId == taskId && db.Equals(reconnect.Database, StringComparison.OrdinalIgnoreCase))
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
                instance.AttachmentStreamsReceived -= OnAttachmentStreamsReceived;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Incoming replication handler has thrown an unhandled exception. ({instance.FromToString})", e);
            }
        }

        private void OnOutgoingSendingFailed(DatabaseOutgoingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.SuccessfulReplication -= ResetReplicationFailuresInfo;

                _outgoing.TryRemove(instance);
                OutgoingReplicationRemoved?.Invoke(instance);

                if (instance is OutgoingPullReplicationHandler)
                    _externalDestinations.Remove(instance.Destination as ExternalReplication);

                if (_outgoingFailureInfo.TryGetValue(instance.Node, out ConnectionShutdownInfo failureInfo) == false)
                    return;

                UpdateLastEtag(instance);

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Document replication connection ({instance.Node}) failed {failureInfo.RetriesCount} times, the connection will be retried on {failureInfo.RetryOn}.", e);

                _reconnectQueue.Add(failureInfo);
            }
        }

        private void UpdateLastEtag(DatabaseOutgoingReplicationHandler instance)
        {
            var etagPerDestination = _lastSendEtagPerDestination.GetOrAdd(
                instance.Node,
                _ => new LastEtagPerDestination());

            if (etagPerDestination.LastEtag == instance._lastSentDocumentEtag)
                return;

            Interlocked.Exchange(ref etagPerDestination.LastEtag, instance._lastSentDocumentEtag);
        }

        private void OnOutgoingSendingSucceeded(DatabaseOutgoingReplicationHandler instance)
        {
            UpdateLastEtag(instance);

            while (_waitForReplicationTasks.TryDequeue(out TaskCompletionSource<object> result))
            {
                TaskExecutor.Complete(result);
            }
        }

        private void ResetReplicationFailuresInfo(DatabaseOutgoingReplicationHandler instance)
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
                var handler = kv.Value as IncomingReplicationHandler;
                if (handler != instance)
                    handler?.OnReplicationFromAnotherSource();
            }
        }

        public override void Dispose()
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

                ea.Execute(() => ConflictResolver?.WaitForBackgroundResolveTask());

                ConflictResolver = null;

                if (_logger.IsInfoEnabled)
                    _logger.Info("Closing and disposing document replication connections.");

                ForTestingPurposes?.BeforeDisposingIncomingReplicationHandlers?.Invoke();
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
            if (minEtag == long.MaxValue)
                return null;

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            switch (tombstoneType)
            {
                case ITombstoneAware.TombstoneType.Documents:
                    result.Add(Constants.Documents.Collections.AllDocumentsCollection, minEtag);
                    break;
                case ITombstoneAware.TombstoneType.TimeSeries:
                    result.Add(Constants.TimeSeries.All, minEtag);
                    break;
                case ITombstoneAware.TombstoneType.Counters:
                    result.Add(Constants.Counters.All, minEtag);
                    break;
                default:
                    throw new NotSupportedException($"Tombstone type '{tombstoneType}' is not supported.");
            }

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

        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
        {
            var dict = new Dictionary<TombstoneDeletionBlockageSource, HashSet<string>>();

            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var _ in Destinations.Where(config => config.Disabled))
                {
                    var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.InternalReplication);
                    dict[source] = tombstoneCollections;
                }
                
                var rawDatabase = _server.Cluster?.ReadRawDatabaseRecord(ctx, Database.Name);
                if (rawDatabase == null)
                    return dict;

                foreach (var config in rawDatabase.ExternalReplications.Where(config => config.Disabled))
                {
                    var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.ExternalReplication, config.Name, config.TaskId);
                    dict[source] = tombstoneCollections;
                }

                foreach (var config in rawDatabase.HubPullReplications.Where(config => config.Disabled))
                {
                    var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.PullReplicationAsHub, config.Name, config.TaskId);
                    dict[source] = tombstoneCollections;
                }

                foreach (var config in rawDatabase.SinkPullReplications.Where(config => config.Disabled))
                {
                    var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.PullReplicationAsSink, config.Name, config.TaskId);
                    dict[source] = tombstoneCollections;
                }
            }

            return dict;
        }

        public sealed class IncomingConnectionRejectionInfo
        {
            public string Reason { get; set; }
            public DateTime When { get; } = DateTime.UtcNow;
        }

        public int GetMinNumberOfReplicas()
        {
            return (_numberOfSiblings + 1) / 2; // not "(_numberOfSiblings + 1) / 2 + 1" because 1 node already have got the data and only need to replicate
        }

        public async Task<int> WaitForReplicationAsync(DocumentsOperationContext context, int numberOfReplicasToWaitFor, TimeSpan waitForReplicasTimeout, ChangeVector lastChangeVector)
        {
            lastChangeVector = lastChangeVector.StripTrxnTags(context);
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var internalDestinations = _internalDestinations.Select(x => x.Url).ToHashSet();
                var waitForNextReplicationAsync = WaitForNextReplicationAsync();
                var past = ReplicatedPastInternalDestinations(context, internalDestinations, lastChangeVector);
                if (past >= numberOfReplicasToWaitFor)
                    return past;

                var remaining = waitForReplicasTimeout - sp.Elapsed;
                if (remaining < TimeSpan.Zero)
                    return ReplicatedPastInternalDestinations(context, internalDestinations, lastChangeVector);

                var timeout = TimeoutManager.WaitFor(remaining);
                try
                {
                    if (await Task.WhenAny(waitForNextReplicationAsync, timeout) == timeout)
                        return ReplicatedPastInternalDestinations(context, internalDestinations, lastChangeVector);
                }
                catch (OperationCanceledException e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Get exception while trying to get write assurance on a database with {numberOfReplicasToWaitFor} servers. " +
                                  $"Written so far to {past} servers only. " +
                                  $"LastChangeVector is: {lastChangeVector}.", e);
                    return ReplicatedPastInternalDestinations(context, internalDestinations, lastChangeVector);
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

        private int ReplicatedPastInternalDestinations(DocumentsOperationContext context, HashSet<string> internalUrls, ChangeVector changeVector)
        {
            var count = 0;

            //We need to avoid the case that we removed database from DB group and CV updated only in the destination
            changeVector.TryRemoveIds(Database.DocumentsStorage.UnusedDatabaseIds, context, out changeVector);

            foreach (var destination in _outgoing)
            {
                if (internalUrls.Contains(destination.Destination.Url) == false)
                    continue;

                var conflictStatus = Database.DocumentsStorage.GetConflictStatusForOrder(context, changeVector, destination.LastAcceptedChangeVector);
                if (conflictStatus == ConflictStatus.AlreadyMerged)
                    count++;
            }

            return count;
        }

        public static bool IsOfTypePreventDeletions(ReplicationBatchItem item)
        {
            switch (item.Type)
            {
                case ReplicationBatchItem.ReplicationItemType.RevisionTombstone:
                case ReplicationBatchItem.ReplicationItemType.AttachmentTombstone:
                case ReplicationBatchItem.ReplicationItemType.DocumentTombstone:
                case ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange:
                    return true;
                case ReplicationBatchItem.ReplicationItemType.Document:
                    if (item is DocumentReplicationItem doc && doc.Flags.Contain(DocumentFlags.DeleteRevision))
                        return true;
                    break;
            }

            return false;
        }

        internal ReplicationProcessProgress GetOutgoingReplicationProgress(DocumentsOperationContext documentsContext, DatabaseOutgoingReplicationHandler handler)
        {
            var lastProcessedEtag = handler.LastSentDocumentEtag;

            var progress = new ReplicationProcessProgress
            {
                FromToString = handler.FromToString,
                LastEtagSent = lastProcessedEtag,
                DestinationChangeVector = handler.LastAcceptedChangeVector,
                SourceChangeVector = handler.LastSentChangeVector,
                AverageProcessedPerSecond = handler.Metrics.GetProcessedPerSecondRate() ?? 0.0
            };

            var collections = Database.DocumentsStorage.GetCollections(documentsContext).Select(x => x.Name);

            long total;
            var overallDuration = Stopwatch.StartNew();

            foreach (var collection in collections)
            {
                progress.NumberOfDocumentsToProcess += Database.DocumentsStorage.GetNumberOfDocumentsToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfDocuments += total;

                progress.NumberOfDocumentTombstonesToProcess += Database.DocumentsStorage.GetNumberOfTombstonesToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfDocumentTombstones += total;

                progress.NumberOfRevisionsToProcess += Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionsToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfRevisions += total;

                progress.NumberOfCounterGroupsToProcess += Database.DocumentsStorage.CountersStorage.GetNumberOfCounterGroupsToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfCounterGroups += total;

                progress.NumberOfTimeSeriesSegmentsToProcess += Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegmentsToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfTimeSeriesSegments += total;

                progress.NumberOfTimeSeriesDeletedRangesToProcess += Database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(documentsContext, collection, lastProcessedEtag, out total, overallDuration);
                progress.TotalNumberOfTimeSeriesDeletedRanges += total;
            }

            progress.NumberOfAttachmentsToProcess = Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachmentsToProcess(documentsContext, lastProcessedEtag, out total, overallDuration);
            progress.TotalNumberOfAttachments = total;
            progress.TotalNumberOfRevisionTombstones = Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionTombstones(documentsContext);
            progress.TotalNumberOfAttachmentTombstones = Database.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachmentTombstones(documentsContext);

            progress.Completed = IsCompleted();

            return progress;

            bool IsCompleted()
            {
                return progress.NumberOfDocumentsToProcess == 0
                       && progress.NumberOfDocumentTombstonesToProcess == 0
                       && progress.NumberOfCounterGroupsToProcess == 0
                       && progress.NumberOfTimeSeriesSegmentsToProcess == 0
                       && progress.NumberOfTimeSeriesDeletedRangesToProcess == 0
                       && progress.NumberOfRevisionsToProcess == 0
                       && progress.NumberOfAttachmentsToProcess == 0;
            }
        }
    }

    public sealed class OutgoingReplicationFailureToConnectReporter : IReportOutgoingReplicationPerformance
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

    public sealed class IncomingReplicationFailureToConnectReporter : IReportIncomingReplicationPerformance
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
