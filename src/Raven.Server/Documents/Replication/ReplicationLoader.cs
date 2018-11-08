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
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
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
using Sparrow;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationLoader : IDisposable, ITombstoneAware
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

        private readonly ConcurrentBag<ReplicationNode> _internalDestinations = new ConcurrentBag<ReplicationNode>();
        private readonly HashSet<ExternalReplication> _externalDestinations = new HashSet<ExternalReplication>();

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
            TcpConnectionHeaderMessage.OperationTypes headerOperation,
            JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var supportedVersions =
                TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Replication, tcpConnectionOptions.ProtocolVersion);

            if (supportedVersions.Replication.SupportPullReplication)
            {
                // wait for replication type
                using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var readerObject = context.ParseToMemory(
                    tcpConnectionOptions.Stream,
                    "initial-replication-message",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    buffer))
                {
                    var initialRequest = JsonDeserializationServer.ReplicationInitialRequest(readerObject);
                    if (initialRequest.PullReplication)
                    {
                        PullReplicationDefinition pullReplicationDefinition;
                        using (_server.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            pullReplicationDefinition = _server.Cluster.ReadPullReplicationDefinition(Database.Name, initialRequest.PullReplicationDefinition, ctx);
                        }

                        var taskId = (int)Hashing.XXHash64.Calculate(initialRequest.DatabaseGroupId, Encodings.Utf8);
                        var externalReplication = pullReplicationDefinition.ToExternalReplication(initialRequest.Database, taskId);
                        var outgoingReplication = new OutgoingReplicationHandler(this, Database, externalReplication, external: true, new[] {initialRequest.Info})
                        {
                            PullReplicationDefinitionName = initialRequest.PullReplicationDefinition
                        };

                        outgoingReplication.Failed += OnOutgoingSendingFailed;
                        outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
                        _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

                        outgoingReplication.StartPullReplicationAsCentral(tcpConnectionOptions.Stream, supportedVersions);
                        OutgoingReplicationAdded?.Invoke(outgoingReplication);
                        return;
                    }
                }
            }

            CreateIncomingInstance(tcpConnectionOptions, buffer);
        }

        public void RunPullReplicationAsEdge(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.ManagedPinnedBuffer buffer, ReplicationNode destination)
        {
            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer);
            //TODO: Incoming need to add, known new value
            IncomingReplicationAdded?.Invoke(newIncoming);
            newIncoming.Failed += RetryPullReplication;

            // Update current thread name
            newIncoming.DoIncomingReplication();

            void RetryPullReplication(IncomingReplicationHandler incomingReplicationHandler, Exception exception)
            {
                // if the stream closed, it is our duty to reconnect
                incomingReplicationHandler.Failed -= RetryPullReplication;
                AddAndStartOutgoingReplication(destination, true);
            }
        }

        public IncomingReplicationHandler CreateIncomingInstance(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var newIncoming = CreateIncomingReplicationHandler(tcpConnectionOptions, buffer);

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

            return newIncoming;
        }

        private IncomingReplicationHandler CreateIncomingReplicationHandler(
            TcpConnectionOptions tcpConnectionOptions, 
            JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var getLatestEtagMessage = IncomingInitialHandshake(tcpConnectionOptions, buffer);

            var newIncoming = new IncomingReplicationHandler(
                tcpConnectionOptions,
                getLatestEtagMessage,
                this,
                buffer);

            newIncoming.Failed += OnIncomingReceiveFailed;
            newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
            return newIncoming;
        }

        private ReplicationLatestEtagRequest IncomingInitialHandshake(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;
            using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var readerObject = context.ParseToMemory(
                tcpConnectionOptions.Stream,
                "IncomingReplication/get-last-etag-message read",
                BlittableJsonDocumentBuilder.UsageMode.None,
                buffer))
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
                incomingConnectionRejectionInfos.Enqueue(new IncomingConnectionRejectionInfo {Reason = e.ToString()});

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

            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}");

            return getLatestEtagMessage;
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
            var instancesToDispose = new List<OutgoingReplicationHandler>();
            if (newRecord == null || _server.IsPassive())
            {
                DropOutgoingConnections(Destinations, instancesToDispose);
                _internalDestinations.Clear();
                _externalDestinations.Clear();
                _destinations.Clear();
                DisposeConnections(instancesToDispose);
                return;
            }

            _clusterTopology = GetClusterTopology();

            HandleInternalReplication(newRecord, instancesToDispose);
            HandleExternalReplication(newRecord, instancesToDispose);
            var destinations = new List<ReplicationNode>();
            destinations.AddRange(_internalDestinations);
            destinations.AddRange(_externalDestinations);
            _destinations = destinations;
            _numberOfSiblings = _destinations.Select(x => x.Url).Intersect(_clusterTopology.AllNodes.Select(x => x.Value)).Count();

            DisposeConnections(instancesToDispose);
        }

        private void DisposeConnections(List<OutgoingReplicationHandler> instancesToDispose)
        {
            TaskExecutor.Execute(toDispose =>
            {
                Parallel.ForEach((List<OutgoingReplicationHandler>)toDispose, instance =>
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
                });
            }, instancesToDispose);
        }

        private (List<ExternalReplication> AddedDestinations, List<ExternalReplication> RemovedDestiantions) FindExternalReplicationChanges(
            HashSet<ExternalReplication> current, List<ExternalReplication> newDestinations)
        {
            if (newDestinations == null)
                newDestinations = new List<ExternalReplication>();

            var addedDestinations = new List<ExternalReplication>();
            var removedDestinations = current.ToList();
            foreach (var newDestination in newDestinations.ToArray())
            {
                if (newDestination.Disabled)
                    continue;

                removedDestinations.Remove(newDestination);
                if (current.Contains(newDestination) == false)
                    addedDestinations.Add(newDestination);
            }

            return (addedDestinations, removedDestinations);
        }

        private void HandleExternalReplication(DatabaseRecord newRecord, List<OutgoingReplicationHandler> instancesToDispose)
        {
            var changes = FindExternalReplicationChanges(_externalDestinations, newRecord.ExternalReplications);

            DropOutgoingConnections(changes.RemovedDestiantions, instancesToDispose);
            var newDestinations = changes.AddedDestinations.Where(configuration =>
            {
                var taskStatus = GetExternalReplicationState(Database, configuration.TaskId);
                var whoseTaskIsIt = Database.WhoseTaskIsIt(newRecord.Topology, configuration, taskStatus);
                return whoseTaskIsIt == _server.NodeTag;
            }).ToList();
            foreach (var externalReplication in newDestinations.ToList())
            {
                if (ValidateConnectionString(newRecord, externalReplication, out var connectionString) == false)
                {
                    newDestinations.Remove(externalReplication);
                    continue;
                }
                externalReplication.ConnectionString = connectionString;
            }
            StartOutgoingConnections(newDestinations, external: true);

            _externalDestinations.RemoveWhere(changes.RemovedDestiantions.Contains);
            foreach (var newDestination in newDestinations)
            {
                _externalDestinations.Add(newDestination);
            }
        }

        public static ExternalReplicationState GetExternalReplicationState(DocumentDatabase database, long taskId)
        {
            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var stateBlittable = database.ServerStore.Cluster.Read(context, ExternalReplicationState.GenerateItemName(database.Name, taskId));

                return stateBlittable != null ? JsonDeserializationCluster.ExternalReplicationState(stateBlittable) : new ExternalReplicationState();
            }
        }

        public void EnsureNotDeleted(string node)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = _server.Cluster.ReadDatabase(ctx, Database.Name, out var _);
                if (record.DeletionInProgress?.ContainsKey(node) == true)
                {
                    throw new OperationCanceledException($"The database '{Database.Name}' on node '{node}' is being deleted, " +
                                                         "so it will not handle replications.");
                }
            }
        }

        public void CompleteDeletionIfNeeded(CancellationTokenSource cts)
        {
            using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = _server.Cluster.ReadDatabase(ctx, Database.Name, out var _);
                if (record?.DeletionInProgress?.ContainsKey(_server.NodeTag) == true)
                {
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
                        TaskExecutor.Execute(state =>
                        {
                            _server.DatabasesLandlord.DeleteDatabase(Database.Name, record.DeletionInProgress[_server.NodeTag], record);
                        }, null);
                    }
                }
            }
        }

        private bool ValidateConnectionString(DatabaseRecord newRecord, ExternalReplication externalReplication, out RavenConnectionString connectionString)
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

        private void HandleInternalReplication(DatabaseRecord newRecord, List<OutgoingReplicationHandler> instancesToDispose)
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

        private void DropOutgoingConnections(IEnumerable<ReplicationNode> connectionsToRemove, List<OutgoingReplicationHandler> instancesToDispose)
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
            return _server.LoadDatabaseRecord(Database.Name, out _);
        }

        internal void AddAndStartOutgoingReplication(ReplicationNode node, bool external)
        {
            var info = GetConnectionInfo(node, external);

            if (info == null)
            {
                // this means that we were unable to retrieve the tcp connection info and will try it again later
                return;
            }
            var outgoingReplication = new OutgoingReplicationHandler(this, Database, node, external, info);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

            outgoingReplication.Start();

            OutgoingReplicationAdded?.Invoke(outgoingReplication);
        }

        private TcpConnectionInfo[] GetConnectionInfo(ReplicationNode node, bool external)
        {
            var shutdownInfo = new ConnectionShutdownInfo
            {
                Node = node,
                External = external
            };
            _outgoingFailureInfo.TryAdd(node, shutdownInfo);
            try
            {
                if (node is ExternalReplication exNode)
                {
                    var database = exNode.ConnectionString.Database;
                    var certificate = GetCertificateForReplication(exNode, out _);

                    if (exNode.PullReplicationAsEdgeSettings == null)
                    {
                        using (var requestExecutor = RequestExecutor.Create(exNode.ConnectionString.TopologyDiscoveryUrls, exNode.ConnectionString.Database, certificate, DocumentConventions.Default))
                        using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        {
                            var cmd = new GetTcpInfoCommand("external-replication", database);
                            requestExecutor.Execute(cmd, ctx);
                            node.Database = database;
                            node.Url = requestExecutor.Url;
                            return new []{ cmd.Result };
                        }
                    }
                   
                    using (var requestExecutor = RequestExecutor.CreateForFixedTopology(exNode.ConnectionString.TopologyDiscoveryUrls, exNode.ConnectionString.Database, certificate, DocumentConventions.Default))
                    using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        var cmd = new GetPullReplicationInfoCommand(database, Database.DatabaseGroupId, exNode.PullReplicationAsEdgeSettings.RemoteName);
                        requestExecutor.Execute(cmd, ctx);
                        node.Database = database;
                        node.Url = requestExecutor.Url;
                        return cmd.Result;
                    }
                }
                if (node is InternalReplication internalNode)
                {
                    using (var cts = new CancellationTokenSource(_server.Engine.TcpConnectionTimeout))
                    {
                        return new[]
                        {
                            ReplicationUtils.GetTcpInfo(internalNode.Url, internalNode.Database, "Replication", _server.Server.Certificate.Certificate, cts.Token)
                        };
                    }
                }
                throw new InvalidOperationException(
                    $"Unexpected replication node type, Expected to be '{typeof(ExternalReplication)}' or '{typeof(InternalReplication)}', but got '{node.GetType()}'");
            }
            catch (Exception e)
            {
                // will try to fetch it again later
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to fetch tcp connection information for the destination '{node.FromString()}' , the connection will be retried later.", e);

                _reconnectQueue.TryAdd(shutdownInfo);
            }
            return null;
        }

        public X509Certificate2 GetCertificateForReplication(ReplicationNode node, out TcpConnectionHeaderMessage.AuthorizationInfo authorizationInfo)
        {
            authorizationInfo = null;
            var exNode = node as ExternalReplication;
            if (exNode == null)
                return _server.Server.Certificate.Certificate;

            if (exNode.PullReplicationAsEdgeSettings?.CertificateThumbprint != null)
            {
                using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var key = Constants.Certificates.FeaturePrefix + exNode.PullReplicationAsEdgeSettings.CertificateThumbprint;
                    var certificate = _server.Cluster.Read(context, key);
                    if (certificate == null)
                    {
                        return _server.Server.Certificate.Certificate;
                    }

                    var definition = JsonDeserializationServer.CertificateDefinition(certificate);
                    authorizationInfo = new TcpConnectionHeaderMessage.AuthorizationInfo
                    {
                        AuthorizeAs = TcpConnectionHeaderMessage.AuthorizationInfo.AuthorizeMethod.PullReplication,
                        RemoteConnectionInfo = exNode.PullReplicationAsEdgeSettings.RemoteName
                    };
                    return new X509Certificate2(Convert.FromBase64String(definition.Certificate), definition.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                }
            }

            return _server.Server.Certificate.Certificate;
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

            if (_log.IsInfoEnabled)
                _log.Info("Closing and disposing document replication connections.");

            foreach (var incoming in _incoming)
                ea.Execute(incoming.Value.Dispose);

            foreach (var outgoing in _outgoing)
                ea.Execute(outgoing.Dispose);

            Database.TombstoneCleaner?.Unsubscribe(this);

            ea.ThrowIfNeeded();
        }

        public string TombstoneCleanerIdentifier => "Replication";

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
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
}
