using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationLoader : IDisposable, IDocumentTombstoneAware
    {
        public event Action<IncomingReplicationHandler> IncomingReplicationAdded;
        public event Action<IncomingReplicationHandler> IncomingReplicationRemoved;

        public event Action<OutgoingReplicationHandler> OutgoingReplicationAdded;
        public event Action<OutgoingReplicationHandler> OutgoingReplicationRemoved;

        internal ManualResetEventSlim DebugWaitAndRunReplicationOnce;

        public readonly DocumentDatabase Database;
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

        private readonly List<ReplicationNode> _internalDestinations = new List<ReplicationNode>();
        private readonly List<ExternalReplication> _externalDestinations = new List<ExternalReplication>();

        private class LastEtagPerDestination
        {
            public long LastEtag;
        }

        private int _replicationStatsId;
        private readonly ConcurrentDictionary<ReplicationNode, LastEtagPerDestination> _lastSendEtagPerDestination =
            new ConcurrentDictionary<ReplicationNode, LastEtagPerDestination>();

        public long MinimalEtagForReplication
        {
            get
            {

                if (Destinations == null || Destinations.Count == 0)
                    return long.MaxValue;

                if (Destinations.Count != _lastSendEtagPerDestination.Count)
                    // if we don't have information from all our destinations, we don't know what tombstones
                    // we can remove. Note that this explicitly _includes_ disabled destinations, which prevents
                    // us from doing any tombstone cleanup.
                    return 0;

                long minEtag = long.MaxValue;
                foreach (var lastEtagPerDestination in _lastSendEtagPerDestination)
                {
                    minEtag = Math.Min(lastEtagPerDestination.Value.LastEtag, minEtag);
                }

                return minEtag;
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

        public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;
            using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var readerObject = context.ParseToMemory(
                tcpConnectionOptions.Stream,
                "IncomingReplication/get-last-etag-message read",
                BlittableJsonDocumentBuilder.UsageMode.None,
                tcpConnectionOptions.PinnedBuffer))
            {
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

                try
                {
                    tcpConnectionOptions.Dispose();
                }
                catch
                {
                    // do nothing
                }

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

                    var lastEtagFromSrc = Database.DocumentsStorage.GetLastReplicateEtagFrom(
                        documentsOperationContext, getLatestEtagMessage.SourceDatabaseId);
                    if (_log.IsInfoEnabled)
                        _log.Info($"GetLastEtag response, last etag: {lastEtagFromSrc}");
                    var response = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = "Ok",
                        [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtagFromSrc,
                        [nameof(ReplicationMessageReply.NodeTag)] = _server.NodeTag,
                        [nameof(ReplicationMessageReply.DatabaseChangeVector)] = changeVector
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

            var newIncoming = new IncomingReplicationHandler(
                tcpConnectionOptions,
                getLatestEtagMessage,
                this);

            newIncoming.Failed += OnIncomingReceiveFailed;
            newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}");

            // need to safeguard against two concurrent connection attempts
            var newConnection = _incoming.GetOrAdd(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming);
            if (newConnection == newIncoming)
            {
                newIncoming.Start();
                IncomingReplicationAdded?.Invoke(newIncoming);
                ForceTryReconnectAll();
            }
            else
                newIncoming.Dispose();
        }

        private void ForceTryReconnectAll()
        {
            foreach (var failure in _reconnectQueue)
            {
                try
                {
                    if (_reconnectQueue.TryRemove(failure) == false)
                        continue;
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

            if (_incoming.TryRemove(connectionInfo.SourceDatabaseId, out IncomingReplicationHandler value))
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Disconnecting existing connection from {value.FromToString} because we got a new connection from the same source db");
                }

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
        }

        private void HandleConflictResolverChange(DatabaseRecord newRecord)
        {
            if (newRecord == null)
            {
                ConflictSolverConfig = null;
                return;
            }

            var conflictSolverChanged = ConflictSolverConfig?.ConflictResolutionChanged(newRecord.ConflictSolverConfig) ?? true;
            if (conflictSolverChanged)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Conflict resolution was change.");
                ConflictSolverConfig = newRecord.ConflictSolverConfig;
                ConflictResolver.RunConflictResolversOnce();
            }
        }

        private void HandleTopologyChange(DatabaseRecord newRecord)
        {
            var instancesToDispose = new List<OutgoingReplicationHandler>();
            if (newRecord == null || _server.IsPassive())
            {
                DropOutgoingConnections(Destinations, ref instancesToDispose);
                _internalDestinations.Clear();
                _externalDestinations.Clear();
                _destinations.Clear();
                DisposeConnections(instancesToDispose);
                return;
            }

            HandleInternalReplication(newRecord, ref instancesToDispose);
            HandleExternalReplication(newRecord, ref instancesToDispose);
            var destinations = new List<ReplicationNode>();
            destinations.AddRange(_internalDestinations);
            destinations.AddRange(_externalDestinations);
            _destinations = destinations;

            DisposeConnections(instancesToDispose);
        }

        private void DisposeConnections(List<OutgoingReplicationHandler> instancesToDispose)
        {
            foreach (var instance in instancesToDispose)
            {
                try
                {
                    instance?.Dispose();
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed to dispose outgoing replication to {instance?.DestinationFormatted}", e);
                }
            }
        }

        private void HandleExternalReplication(DatabaseRecord newRecord, ref List<OutgoingReplicationHandler> instancesToDispose)
        {
            var changes = ExternalReplication.FindChanges(_externalDestinations, newRecord.ExternalReplication);
            if (changes.RemovedDestiantions.Count > 0)
            {
                var removed = _externalDestinations.Where(n => changes.RemovedDestiantions.Contains(n.Url + "@" + n.Database));
                DropOutgoingConnections(removed, ref instancesToDispose);
            }
            if (changes.AddedDestinations.Count > 0)
            {
                var added = newRecord.ExternalReplication.Where(n => changes.AddedDestinations.Contains(n.Url + "@" + n.Database));
                StartOutgoingConnections(added.ToList(), external: true);
            }
            _externalDestinations.Clear();
            _externalDestinations.AddRange(newRecord.ExternalReplication);
        }

        private void HandleInternalReplication(DatabaseRecord newRecord, ref List<OutgoingReplicationHandler> instancesToDispose)
        {
            var clusterTopology = GetClusterTopology();
            var newInternalDestinations = newRecord.Topology?.GetDestinations(_server.NodeTag, Database.Name, clusterTopology, _server.IsPassive());
            var internalConnections = DatabaseTopology.FindChanges(_internalDestinations, newInternalDestinations);

            if (internalConnections.RemovedDestiantions.Count > 0)
            {
                var removed = internalConnections.RemovedDestiantions.Select(r => new InternalReplication
                {
                    NodeTag = clusterTopology.TryGetNodeTagByUrl(r).NodeTag,
                    Url = r,
                    Database = Database.Name
                });

                DropOutgoingConnections(removed, ref instancesToDispose);
            }
            if (internalConnections.AddedDestinations.Count > 0)
            {
                var added = internalConnections.AddedDestinations.Select(r => new InternalReplication
                {
                    NodeTag = clusterTopology.TryGetNodeTagByUrl(r).NodeTag,
                    Url = r,
                    Database = Database.Name
                });
                StartOutgoingConnections(added.ToList());
            }
            _internalDestinations.Clear();
            _internalDestinations.AddRange(newInternalDestinations);
        }

        private void StartOutgoingConnections(IReadOnlyCollection<ReplicationNode> connectionsToAdd, bool external = false)
        {
            if (connectionsToAdd.Count == 0)
                return;

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

        private void DropOutgoingConnections(IEnumerable<ReplicationNode> connectionsToRemove, ref List<OutgoingReplicationHandler> instancesToDispose)
        {
            var outgoingChanged = _outgoing.Where(o => connectionsToRemove.Contains(o.Destination)).ToList();
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
                instancesToDispose.Add(instance);
                _outgoing.TryRemove(instance);
                _lastSendEtagPerDestination.TryRemove(instance.Destination, out LastEtagPerDestination _);
                _outgoingFailureInfo.TryRemove(instance.Destination, out ConnectionShutdownInfo info);
                if (info != null)
                    _reconnectQueue.TryRemove(info);
            }
        }

        public DatabaseRecord LoadDatabaseRecord()
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _server.Cluster.ReadDatabase(context, Database.Name);
            }
        }

        private void AddAndStartOutgoingReplication(ReplicationNode node, bool external)
        {
            var outgoingReplication = new OutgoingReplicationHandler(this, Database, node, external);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

            if (node is ExternalReplication exNode)
            {
                try
                {
                    using (var requestExecutor = RequestExecutor.Create(exNode.TopologyDiscoveryUrls, exNode.Database, _server.Server.ClusterCertificateHolder.Certificate,
                        DocumentConventions.Default))
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        var cmd = new GetTcpInfoCommand("extrenal-replication");
                        requestExecutor.Execute(cmd, ctx);
                        node.Url = requestExecutor.Url;
                        outgoingReplication.ConnectionInfo = cmd.Result;
                    }
                }
                catch (Exception e)
                {
                    // will try to fetch it again later
                    if (_log.IsInfoEnabled)
                        _log.Info($"External replication failed to fetch connection information for database '{node.Database}', the connection will be retried later.", e);

                    _reconnectQueue.TryAdd(new ConnectionShutdownInfo
                    {
                        Node = node,
                        External = external
                    });
                    return;
                }
            }
            else
            {
                node.Url = node.Url.Trim();
            }

            _outgoingFailureInfo.TryAdd(node, new ConnectionShutdownInfo
            {
                Node = node,
                External = external
            });
            outgoingReplication.Start();

            OutgoingReplicationAdded?.Invoke(outgoingReplication);
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

                _outgoing.TryRemove(instance);
                OutgoingReplicationRemoved?.Invoke(instance);

                if (_outgoingFailureInfo.TryGetValue(instance.Node, out ConnectionShutdownInfo failureInfo) == false)
                    return;

                UpdateLastEtag(instance);

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;

                _reconnectQueue.Add(failureInfo);

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Node}) failed, and the connection will be retried later.", e);

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

            if (_outgoingFailureInfo.TryGetValue(instance.Node, out ConnectionShutdownInfo failureInfo))
                failureInfo.Reset();
            while (_waitForReplicationTasks.TryDequeue(out TaskCompletionSource<object> result))
            {
                TaskExecutor.Complete(result);
            }
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
            var ea = new ExceptionAggregator("Failed during dispose of document replication loader");

            ea.Execute(_reconnectAttemptTimer.Dispose);

            ea.Execute(() => ConflictResolver?.ResolveConflictsTask.Wait());

            if (_log.IsInfoEnabled)
                _log.Info("Closing and disposing document replication connections.");

            foreach (var incoming in _incoming)
                ea.Execute(incoming.Value.Dispose);

            foreach (var outgoing in _outgoing)
                ea.Execute(outgoing.Dispose);

            Database.DocumentTombstoneCleaner?.Unsubscribe(this);

            ea.ThrowIfNeeded();
        }

        public Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            var minEtag = MinimalEtagForReplication;
            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
            {
                {Constants.Documents.Collections.AllDocumentsCollection, minEtag}
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

            if (!tooManyTombstones)
                return result;

            Database.NotificationCenter.Add(
                PerformanceHint.Create(
                    title: "Large number of tombstones because of disabled replication destination",
                    msg:
                        $"The disabled replication destination {disabledReplicationNode.FromString()} prevents from cleaning large number of tombstones.",

                    type: PerformanceHintType.Replication,
                    notificationSeverity: NotificationSeverity.Warning,
                    source: disabledReplicationNode.FromString()
                ));

            return result;
        }

        public class IncomingConnectionRejectionInfo
        {
            public string Reason { get; set; }
            public DateTime When { get; } = DateTime.UtcNow;
        }

        public class ConnectionShutdownInfo
        {
            public string DestinationDbId;

            public bool External;

            public long LastHeartbeatTicks;

            public const int MaxConnectionTimeout = 60000;

            public readonly Queue<Exception> Errors = new Queue<Exception>();

            public TimeSpan NextTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

            public DateTime RetryOn { get; set; }

            public ReplicationNode Node { get; set; }

            public void Reset()
            {
                NextTimeout = TimeSpan.FromMilliseconds(500);
                Errors.Clear();
            }

            public void OnError(Exception e)
            {
                Errors.Enqueue(e);
                while (Errors.Count > 25)
                    Errors.TryDequeue(out _);

                NextTimeout = TimeSpan.FromMilliseconds(Math.Min(NextTimeout.TotalMilliseconds * 4, MaxConnectionTimeout));
                RetryOn = DateTime.UtcNow + NextTimeout;
            }
        }

        public int GetSizeOfMajority()
        {
            var numberOfSiblings = _destinations.Count;
            return numberOfSiblings / 2 + 1;
        }

        public async Task<int> WaitForReplicationAsync(int numberOfReplicasToWaitFor, TimeSpan waitForReplicasTimeout, string lastChangeVector)
        {
            var numberOfSiblings = _destinations.Count;
            if (numberOfSiblings == 0)
            {
             
                if (_log.IsInfoEnabled)
                    _log.Info("Was asked to get write assurance on a database without replication, ignoring the request. " +
                              $"InternalDestinations: {_internalDestinations.Count}. " +
                              $"ExternalDestinations: {_externalDestinations.Count}. " +
                              $"Destinations: {_destinations.Count}.");

                return numberOfReplicasToWaitFor;
            }
            if (numberOfSiblings < numberOfReplicasToWaitFor)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Was asked to get write assurance on a database with {numberOfReplicasToWaitFor} servers " +
                              $"but we have only {numberOfSiblings} servers, reducing request to {numberOfSiblings}. " +
                              $"InternalDestinations: {_internalDestinations.Count}. " +
                              $"ExternalDestinations: {_externalDestinations.Count}. " +
                              $"Destinations: {_destinations.Count}.");

                numberOfReplicasToWaitFor = numberOfSiblings;
            }
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var waitForNextReplicationAsync = WaitForNextReplicationAsync();
                var past = ReplicatedPast(lastChangeVector);
                if (past >= numberOfReplicasToWaitFor)
                    return past;

                var remaining = waitForReplicasTimeout - sp.Elapsed;
                if (remaining < TimeSpan.Zero)
                    return ReplicatedPast(lastChangeVector);

                var timeout = TimeoutManager.WaitFor(remaining);
                try
                {
                    if (await Task.WhenAny(waitForNextReplicationAsync, timeout) == timeout)
                        return ReplicatedPast(lastChangeVector);
                }
                catch (OperationCanceledException e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Get exception while trying to get write assurance on a database with {numberOfReplicasToWaitFor} servers. " +
                                  $"Written so far to {past} servers only. " +
                                  $"LastChangeVector is: {lastChangeVector}.", e);
                    return ReplicatedPast(lastChangeVector);
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

        public int GetNextReplicationStatsId()
        {
            return Interlocked.Increment(ref _replicationStatsId);
        }
    }
}
