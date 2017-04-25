using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
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
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationLoader : IDisposable, IDocumentTombstoneAware
    {
        public event Action<string, Exception> ReplicationFailed;

        public event Action<string, IncomingReplicationHandler> IncomingReplicationAdded;
        public event Action<string> IncomingReplicationRemoved;

        public event Action<OutgoingReplicationHandler> OutgoingReplicationAdded;
        public event Action<OutgoingReplicationHandler> OutgoingReplicationRemoved;

        public readonly DocumentDatabase Database;
        private volatile bool _isInitialized;

        private readonly Timer _reconnectAttemptTimer;
        internal int MinimalHeartbeatInterval;

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
        private int _numberOfSiblings;

        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Values.Select(x => x.ConnectionInfo);
        public IEnumerable<ReplicationNode> OutgoingConnections => _outgoing.Select(x => x.Node);
        public IEnumerable<OutgoingReplicationHandler> OutgoingHandlers => _outgoing;
        public IEnumerable<IncomingReplicationHandler> IncomingHandlers => _incoming.Values;

        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks =
            new ConcurrentQueue<TaskCompletionSource<object>>();

        private readonly ServerStore _server;
      
        public List<ReplicationNode> Destinations => _destinations ?? new List<ReplicationNode>();
        private List<ReplicationNode> _destinations;
        public ConflictSolver ConflictSolverConfig;

        public ReplicationLoader(DocumentDatabase database, ServerStore server)
        {
            _server = server;
            Database = database;
            _log = LoggingSource.Instance.GetLogger<ReplicationLoader>(Database.Name);
            _reconnectAttemptTimer = new Timer(AttemptReconnectFailedOutgoing,
                null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
            MinimalHeartbeatInterval =
               (int)Database.Configuration.Replication.ReplicationMinimalHeartbeat.AsTimeSpan.TotalMilliseconds;

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
                if (replicationHandler.Node.IsMatch(dest))
                    return replicationHandler._lastSentDocumentEtag;
            }
            return null;
        }

        public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;
            JsonOperationContext context;
            using (tcpConnectionOptions.ContextPool.AllocateOperationContext(out context))
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
                        $"GetLastEtag: {getLatestEtagMessage.SourceMachineName} / {getLatestEtagMessage.SourceDatabaseName} ({getLatestEtagMessage.SourceDatabaseId}) - {getLatestEtagMessage.SourceUrl}");
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
                DocumentsOperationContext documentsOperationContext;
                TransactionOperationContext configurationContext;
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsOperationContext))
                using (Database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
                using (var writer = new BlittableJsonTextWriter(documentsOperationContext, tcpConnectionOptions.Stream))
                using (documentsOperationContext.OpenReadTransaction())
                using (var configTx = configurationContext.OpenReadTransaction())
                {
                    var documentsChangeVector = new DynamicJsonArray();
                    foreach (
                        var changeVectorEntry in
                        Database.DocumentsStorage.GetDatabaseChangeVector(documentsOperationContext))
                    {
                        documentsChangeVector.Add(new DynamicJsonValue
                        {
                            [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                            [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                        });
                    }

                    var indexesChangeVector = new DynamicJsonArray();
                    var changeVectorAsArray =
                        Database.IndexMetadataPersistence.GetIndexesAndTransformersChangeVector(
                            configTx.InnerTransaction);
                    foreach (var changeVectorEntry in changeVectorAsArray)
                    {
                        indexesChangeVector.Add(new DynamicJsonValue
                        {
                            [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                            [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                        });
                    }

                    var lastEtagFromSrc = Database.DocumentsStorage.GetLastReplicateEtagFrom(
                        documentsOperationContext, getLatestEtagMessage.SourceDatabaseId);
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"GetLastEtag response, last etag: {lastEtagFromSrc}");
                    }
                    var response = new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = "Ok",
                        [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtagFromSrc,
                        [nameof(ReplicationMessageReply.LastIndexTransformerEtagAccepted)] =
                        Database.IndexMetadataPersistence.GetLastReplicateEtagFrom(configTx.InnerTransaction,
                            getLatestEtagMessage.SourceDatabaseId),
                        [nameof(ReplicationMessageReply.DocumentsChangeVector)] = documentsChangeVector,
                        [nameof(ReplicationMessageReply.IndexTransformerChangeVector)] = indexesChangeVector,
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
                IncomingReplicationAdded?.Invoke(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming);
            }
            else
                newIncoming.Dispose();
        }

        private void AttemptReconnectFailedOutgoing(object state)
        {
            var minDiff = TimeSpan.FromSeconds(30);
            foreach (var failure in _reconnectQueue)
            {
                var diff = failure.RetryOn - DateTime.UtcNow;
                if (diff < TimeSpan.Zero)
                {
                    try
                    {
                        _reconnectQueue.TryRemove(failure);
                        AddAndStartOutgoingReplication(failure.Node);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Failed to start outgoing replication to {failure.Node}", e);
                        }
                    }
                }
                else
                {
                    if (minDiff > diff)
                        minDiff = diff;
                }
            }

            try
            {
                //at this stage we can be already disposed, so ...
                _reconnectAttemptTimer.Change(minDiff, TimeSpan.FromDays(1));
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void AssertValidConnection(IncomingConnectionInfo connectionInfo)
        {
            Guid sourceDbId;
            //precaution, should never happen..
            if (string.IsNullOrWhiteSpace(connectionInfo.SourceDatabaseId) ||
                !Guid.TryParse(connectionInfo.SourceDatabaseId, out sourceDbId))
            {
                throw new InvalidOperationException(
                    $"Failed to parse source database Id. What I got is {(string.IsNullOrWhiteSpace(connectionInfo.SourceDatabaseId) ? "<empty string>" : Database.DbId.ToString())}. This is not supposed to happen and is likely a bug.");
            }

            if (sourceDbId == Database.DbId)
            {
                throw new InvalidOperationException(
                    $"Cannot have replication with source and destination being the same database. They share the same db id ({connectionInfo} - {Database.DbId})");
            }

            IncomingReplicationHandler value;
            if (_incoming.TryRemove(connectionInfo.SourceDatabaseId, out value))
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Disconnecting existing connection from {value.FromToString} because we got a new connection from the same source db");
                }

                IncomingReplicationRemoved?.Invoke(connectionInfo.SourceDatabaseId);

                value.Dispose();
            }
        }

        public void Initialize()
        {
            if (_isInitialized) //precaution -> probably not necessary, but still...
                return;

            var record = LoadDatabaseRecord();

            ConflictSolverConfig = record?.ConflictSolverConfig;
            ConflictResolver = new ResolveConflictOnReplicationConfigurationChange(this, _log);
            ConflictResolver.RunConflictResolversOnce();

            lock (_locker)
            {
                _destinations = record?.Topology?.GetDestinations(_server.NodeTag, Database.Name);
                InitializeOutgoingReplications();
            }
            _isInitialized = true;
        }

        private readonly object _locker = new object();

        public void HandleDatabaseRecordChange()
        {
            var newRecord = LoadDatabaseRecord();
            HandleConflictResolverChange(newRecord);
            lock (_locker)
            {
                HandleTopologyChange(newRecord);
            }
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
            if (newRecord == null)
            {
                DropOutgoingConnections(Destinations);
                _destinations = null;
                return;
            }

            var newDestinations = newRecord.Topology.GetDestinations(_server.NodeTag, Database.Name);
            var connectionChanged = DatabaseTopology.FindConnectionChanges(_destinations, newDestinations);
            _destinations = newDestinations;

            if (connectionChanged.nodesToRemove.Count > 0)
            {
                DropOutgoingConnections(connectionChanged.nodesToRemove);
            }
            if (connectionChanged.nodesToAdd.Count > 0)
            {
                StartOutgoingConnections(connectionChanged.nodesToAdd);
            }
        }

        private void StartOutgoingConnections(IReadOnlyCollection<ReplicationNode> connectionsToAdd)
        {
            if (connectionsToAdd.Count == 0)
                return;

            if (_log.IsInfoEnabled)
                _log.Info($"Initializing {connectionsToAdd.Count:#,#} outgoing replications from {Database} on {_server.NodeTag}.");

            foreach (var destination in connectionsToAdd)
            {
                if (destination.Disabled)
                    continue;

                _numberOfSiblings++;
                if (_log.IsInfoEnabled)
                    _log.Info($"Initialized outgoing replication for [{destination.NodeTag}/{destination.Url}]");
                AddAndStartOutgoingReplication(destination);
            }

            if (_log.IsInfoEnabled)
                _log.Info("Finished initialization of outgoing replications..");
        }

        private void DropOutgoingConnections(ICollection<ReplicationNode> connectionsToRemove)
        {
            var outgoingChanged = _outgoing.Where(o => connectionsToRemove.Contains(o.Destination)).ToList();
            if (outgoingChanged.Count == 0)
                return; // no connections to remove

            if (_log.IsInfoEnabled)
                _log.Info($"Dropping {outgoingChanged.Count:#,#} outgoing replications connections from {Database} on {_server.NodeTag}.");

            foreach (var instance in outgoingChanged)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Stopping replication to {instance.Destination.Database} on {instance.Destination.NodeTag}.");

                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.Dispose();
                _outgoing.TryRemove(instance);
                _lastSendEtagPerDestination.TryRemove(instance.Destination, out LastEtagPerDestination etag);
                _outgoingFailureInfo.TryRemove(instance.Destination, out ConnectionShutdownInfo info);
                _reconnectQueue.TryRemove(info);
                _numberOfSiblings--;
            }
        }

        private void InitializeOutgoingReplications()
        {
            if (Destinations.Count == 0)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Tried to initialize outgoing replications, but there is no replication document or destinations are empty. Nothing to do...");

                _numberOfSiblings = 0;
                Database.DocumentTombstoneCleaner?.Unsubscribe(this);
                return;
            }

            Database.DocumentTombstoneCleaner.Subscribe(this);

            _numberOfSiblings = 0;
            StartOutgoingConnections(Destinations);
        }

        private DatabaseRecord LoadDatabaseRecord()
        {
            TransactionOperationContext context;
            using (_server.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                return _server.Cluster.ReadDatabase(context, Database.Name);
            }
        }

        private void AddAndStartOutgoingReplication(ReplicationNode node)
        {
            var outgoingReplication = new OutgoingReplicationHandler(this, Database, node);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

            node.Url = node.Url.Trim();

            _outgoingFailureInfo.TryAdd(node, new ConnectionShutdownInfo
            {
                Node = node
            });
            outgoingReplication.Start();

            OutgoingReplicationAdded?.Invoke(outgoingReplication);
        }

        private void OnIncomingReceiveFailed(IncomingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                IncomingReplicationHandler _;
                if (_incoming.TryRemove(instance.ConnectionInfo.SourceDatabaseId, out _))
                    IncomingReplicationRemoved?.Invoke(instance.ConnectionInfo.SourceDatabaseId);

                instance.Failed -= OnIncomingReceiveFailed;
                instance.DocumentsReceived -= OnIncomingReceiveSucceeded;
                if (_log.IsInfoEnabled)
                    _log.Info($"Incoming replication handler has thrown an unhandled exception. ({instance.FromToString})", e);

                ReplicationFailed?.Invoke(instance.FromToString, e);
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

                ConnectionShutdownInfo failureInfo;
                if (_outgoingFailureInfo.TryGetValue(instance.Node, out failureInfo) == false)
                    return;

                UpdateLastEtag(instance);

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;
                failureInfo.LastAcceptedDocumentEtag = instance.LastAcceptedDocumentEtag;
                failureInfo.LastSentIndexOrTransformerEtag = instance._lastSentIndexOrTransformerEtag;

                _reconnectQueue.Add(failureInfo);

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Node}) failed, and the connection will be retried later.",
                        e);

                ReplicationFailed?.Invoke(instance.Node.ToString(), e);
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

            ConnectionShutdownInfo failureInfo;
            if (_outgoingFailureInfo.TryGetValue(instance.Node, out failureInfo))
                failureInfo.Reset();
            TaskCompletionSource<object> result;
            while (_waitForReplicationTasks.TryDequeue(out result))
            {
                TaskExecuter.Complete(result);
            }

        }

        private void OnIncomingReceiveSucceeded(IncomingReplicationHandler instance)
        {
            _incomingLastActivityTime.AddOrUpdate(instance.ConnectionInfo, DateTime.UtcNow, (_, __) => DateTime.UtcNow);
            foreach (var handler in _incoming.Values)
            {
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
                {Constants.Documents.Replication.AllDocumentsCollection, minEtag}
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
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
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
                        $"The disabled replication destination {disabledReplicationNode.NodeTag} on " +
                        $"{disabledReplicationNode.Url} prevents from cleaning large number of tombstones.",

                    type: PerformanceHintType.Replication,
                    notificationSeverity: NotificationSeverity.Warning,
                    source: $"{disabledReplicationNode.NodeTag} on {disabledReplicationNode.Url}"
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

            public long LastAcceptedDocumentEtag;
            public long LastSentIndexOrTransformerEtag;

            public long LastHeartbeatTicks;

            public const int MaxConnectionTimout = 60000;

            public int ErrorCount { get; set; }

            public TimeSpan NextTimout { get; set; } = TimeSpan.FromMilliseconds(500);

            public DateTime RetryOn { get; set; }

            public ReplicationNode Node { get; set; }

            public void Reset()
            {
                NextTimout = TimeSpan.FromMilliseconds(500);
                ErrorCount = 0;
            }

            public void OnError(Exception e)
            {
                ErrorCount++;
                NextTimout = TimeSpan.FromMilliseconds(Math.Min(NextTimout.TotalMilliseconds * 4, MaxConnectionTimout));
                RetryOn = DateTime.UtcNow + NextTimout;
                LastException = e;
            }

            public Exception LastException { get; set; }
        }

        public int GetSizeOfMajority()
        {
            return _numberOfSiblings / 2 + 1;
        }

        public async Task<int> WaitForReplicationAsync(
            int numberOfReplicasToWaitFor,
            TimeSpan waitForReplicasTimeout,
            long lastEtag)
        {
            if (_numberOfSiblings == 0)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Was asked to get write assurance on a database without replication, ignoring the request");
                }
                return numberOfReplicasToWaitFor;
            }
            if (_numberOfSiblings < numberOfReplicasToWaitFor)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Was asked to get write assurance on a database with {numberOfReplicasToWaitFor} servers but we have only {_numberOfSiblings} servers, reducing request to {_numberOfSiblings}");
                }
                numberOfReplicasToWaitFor = _numberOfSiblings;
            }
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var waitForNextReplicationAsync = WaitForNextReplicationAsync();
                var past = ReplicatedPast(lastEtag);
                if (past >= numberOfReplicasToWaitFor)
                    return past;

                var remaining = waitForReplicasTimeout - sp.Elapsed;
                if (remaining < TimeSpan.Zero)
                    return ReplicatedPast(lastEtag);

                var timeout = Task.Delay(remaining);
                try
                {
                    if (await Task.WhenAny(waitForNextReplicationAsync, timeout) == timeout)
                    {
                        return ReplicatedPast(lastEtag);
                    }
                }
                catch (OperationCanceledException)
                {
                    return ReplicatedPast(lastEtag);
                }
            }
        }

        private Task WaitForNextReplicationAsync()
        {
            TaskCompletionSource<object> result;
            if (_waitForReplicationTasks.TryPeek(out result))
                return result.Task;

            result = new TaskCompletionSource<object>();
            _waitForReplicationTasks.Enqueue(result);
            return result.Task;
        }

        private int ReplicatedPast(long etag)
        {
            int count = 0;
            foreach (var destination in _outgoing)
            {
                if (destination.LastAcceptedDocumentEtag >= etag)
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
