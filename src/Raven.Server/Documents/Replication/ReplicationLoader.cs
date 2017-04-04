using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.Utils;

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

        public ResolveConflictOnReplicationConfigurationChange ConflictResolver { get; }

        private readonly ConcurrentSet<OutgoingReplicationHandler> _outgoing =
            new ConcurrentSet<OutgoingReplicationHandler>();

        private readonly ConcurrentDictionary<ReplicationDestination, ConnectionShutdownInfo> _outgoingFailureInfo =
            new ConcurrentDictionary<ReplicationDestination, ConnectionShutdownInfo>();

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

        private readonly ConcurrentDictionary<ReplicationDestination, LastEtagPerDestination> _lastSendEtagPerDestination =
            new ConcurrentDictionary<ReplicationDestination, LastEtagPerDestination>();

        public long MinimalEtagForReplication
        {
            get
            {
                var replicationDocument = ReplicationDocument;// thread safe copy

                if (replicationDocument?.Destinations == null || replicationDocument.Destinations.Count == 0)
                    return long.MaxValue;

                if (replicationDocument.Destinations.Count != _lastSendEtagPerDestination.Count)
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
        internal ReplicationDocument ReplicationDocument;
        private int _numberOfSiblings;

        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Values.Select(x => x.ConnectionInfo);
        public IEnumerable<ReplicationDestination> OutgoingConnections => _outgoing.Select(x => x.Destination);
        public IEnumerable<OutgoingReplicationHandler> OutgoingHandlers => _outgoing;
        public IEnumerable<IncomingReplicationHandler> IncomingHandlers => _incoming.Values;

        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks =
            new ConcurrentQueue<TaskCompletionSource<object>>();

        public ReplicationLoader(DocumentDatabase database)
        {
            Database = database;
            _log = LoggingSource.Instance.GetLogger<ReplicationLoader>(Database.Name);
            _reconnectAttemptTimer = new Timer(AttemptReconnectFailedOutgoing,
                null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
            MinimalHeartbeatInterval =
               (int)Database.Configuration.Replication.ReplicationMinimalHeartbeat.AsTimeSpan.TotalMilliseconds;
            ConflictResolver = new ResolveConflictOnReplicationConfigurationChange(this, _log);
        }

        public IReadOnlyDictionary<ReplicationDestination, ConnectionShutdownInfo> OutgoingFailureInfo
            => _outgoingFailureInfo;

        public IReadOnlyDictionary<IncomingConnectionInfo, DateTime> IncomingLastActivityTime
            => _incomingLastActivityTime;

        public IReadOnlyDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>
            IncomingRejectionStats => _incomingRejectionStats;

        public IEnumerable<ReplicationDestination> ReconnectQueue => _reconnectQueue.Select(x => x.Destination);

        public long? GetLastReplicatedEtagForDestination(ReplicationDestination dest)
        {
            foreach (var replicationHandler in _outgoing)
            {
                if (replicationHandler.Destination.IsMatch(dest))
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
                UpdateReplicationDocumentWithResolver(
                    getLatestEtagMessage.ResolverId,
                    getLatestEtagMessage.ResolverVersion);
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
                        [nameof(ReplicationMessageReply.ResolverId)] =
                        ReplicationDocument?.DefaultResolver?.ResolvingDatabaseId,
                        [nameof(ReplicationMessageReply.ResolverVersion)] =
                        ReplicationDocument?.DefaultResolver?.Version,
                        [nameof(ReplicationMessageReply.DatabaseId)] = Database.DbId.ToString()
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
                        AddAndStartOutgoingReplication(failure.Destination);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Failed to start outgoing replication to {failure.Destination}", e);
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

            _isInitialized = true;

            Database.Changes.OnSystemDocumentChange += OnSystemDocumentChange;

            InitializeOutgoingReplications();
            ConflictResolver.RunConflictResolversOnce();
        }

        private void InitializeOutgoingReplications()
        {
            ReplicationDocument = GetReplicationDocument();

            if (ValidateReplicaitonSource() == false)
                return;

            if (ReplicationDocument?.Destinations == null || //precaution
                ReplicationDocument.Destinations.Count == 0)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Tried to initialize outgoing replications, but there is no replication document or destinations are empty. Nothing to do...");

                _numberOfSiblings = 0;
                Database.DocumentTombstoneCleaner.Unsubscribe(this);

                return;
            }

            Database.DocumentTombstoneCleaner.Subscribe(this);

            if (_log.IsInfoEnabled)
                _log.Info($"Initializing {ReplicationDocument.Destinations.Count:#,#} outgoing replications..");

            var countOfDestinations = 0;
            foreach (var destination in ReplicationDocument.Destinations)
            {
                if (destination.Disabled)
                    continue;

                countOfDestinations++;

                AddAndStartOutgoingReplication(destination);
                if (_log.IsInfoEnabled)
                    _log.Info($"Initialized outgoing replication for [{destination.Database}/{destination.Url}]");
            }

            _numberOfSiblings = countOfDestinations;

            if (_log.IsInfoEnabled)
                _log.Info("Finished initialization of outgoing replications..");
        }

        private readonly AlertRaised _databaseMismatchAlert = AlertRaised.Create(
                   "Replication source mismatch",
                   $"Replication source does not match this database, outgoing replication is disabled until this will be fixed at {Constants.Documents.Replication.ReplicationConfigurationDocument}.",
                   AlertType.Replication,
                   NotificationSeverity.Error,
                   "DatabaseMismatch"
               );

        private bool ValidateReplicaitonSource()
        {
            var replicationDocument = ReplicationDocument;
            if (replicationDocument == null)
            {
                return true;
            }

            if (replicationDocument.Source == null)
            {
                replicationDocument.Source = Database.DbId.ToString();
                DocumentsOperationContext context;
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var djv = replicationDocument.ToJson();
                    using (var doc = context.ReadObject(djv,
                        Constants.Documents.Replication.ReplicationConfigurationDocument))
                    {
                        Database.DocumentsStorage.Put(context, Constants.Documents.Replication.ReplicationConfigurationDocument,
                            null, doc);
                        tx.Commit();
                    }
                }
                // this is going to return false, because we modifed the configuration doc
                // which will cause the code to recurse, so we just avoid running this instance
                // because the instance we just run because we committed the update have already
                // done all the work
                return false;
            }

            Guid sourceDbId;
            if (Guid.TryParse(replicationDocument.Source, out sourceDbId) == false ||
                sourceDbId != Database.DbId)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Replication source '{replicationDocument.Source}' does not match this database, outgoing replication is disabled until this will be fixed.");

                Database.NotificationCenter.Add(_databaseMismatchAlert);
                return false;
            }
            Database.NotificationCenter.Dismiss(_databaseMismatchAlert.Id);
            return true;
        }

        private void AddAndStartOutgoingReplication(ReplicationDestination destination)
        {
            var outgoingReplication = new OutgoingReplicationHandler(this, Database, destination);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance

            destination.Url = destination.Url.Trim();

            _outgoingFailureInfo.TryAdd(destination, new ConnectionShutdownInfo
            {
                Destination = destination
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
                if (_outgoingFailureInfo.TryGetValue(instance.Destination, out failureInfo) == false)
                    return;

                UpdateLastEtag(instance);

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;
                failureInfo.LastAcceptedDocumentEtag = instance.LastAcceptedDocumentEtag;
                failureInfo.LastSentIndexOrTransformerEtag = instance._lastSentIndexOrTransformerEtag;

                _reconnectQueue.Add(failureInfo);

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Destination}) failed, and the connection will be retried later.",
                        e);

                ReplicationFailed?.Invoke(instance.Destination.ToString(), e);
            }
        }

        private void UpdateLastEtag(OutgoingReplicationHandler instance)
        {
            var etagPerDestination = _lastSendEtagPerDestination.GetOrAdd(
                instance.Destination,
                _ => new LastEtagPerDestination());

            if (etagPerDestination.LastEtag == instance._lastSentDocumentEtag)
                return;

            Interlocked.Exchange(ref etagPerDestination.LastEtag, instance._lastSentDocumentEtag);
        }

        private void OnOutgoingSendingSucceeded(OutgoingReplicationHandler instance)
        {
            UpdateLastEtag(instance);

            ConnectionShutdownInfo failureInfo;
            if (_outgoingFailureInfo.TryGetValue(instance.Destination, out failureInfo))
                failureInfo.Reset();
            TaskCompletionSource<object> result;
            while (_waitForReplicationTasks.TryDequeue(out result))
            {
                ThreadPool.QueueUserWorkItem(task => ((TaskCompletionSource<object>)task).TrySetResult(null), result);
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

        private void OnSystemDocumentChange(DocumentChange change)
        {
            if (!change.Key.Equals(Constants.Documents.Replication.ReplicationConfigurationDocument, StringComparison.OrdinalIgnoreCase))
                return;

            if (_log.IsInfoEnabled)
                _log.Info("System document change detected. Starting and stopping outgoing replication threads.");

            //prevent reconnecting to a destination that we shouldn't in case we have flaky network
            _reconnectQueue.Clear();

            foreach (var instance in _outgoing)
            {
                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;

                OutgoingReplicationRemoved?.Invoke(instance);

                instance.Dispose();
            }

            _outgoing.Clear();
            _outgoingFailureInfo.Clear();
            _lastSendEtagPerDestination.Clear();

            InitializeOutgoingReplications();

            ConflictResolver.RunConflictResolversOnce();

            if (_log.IsInfoEnabled)
                _log.Info($"Replication configuration was changed: {change.Key}");
        }

        internal void UpdateReplicationDocumentWithResolver(string uid, int? version)
        {
            if (version == null || ReplicationDocument?.DefaultResolver?.Version > version)
            {
                return; // nothing to do
            }

            if (ReplicationDocument?.DefaultResolver != null &&
                ReplicationDocument.DefaultResolver.Version == version &&
                ReplicationDocument.DefaultResolver.ResolvingDatabaseId != uid)
                ThrowConflictingResolvers(uid, version, ReplicationDocument.DefaultResolver.ResolvingDatabaseId);

            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                var configurationDocument = Database.DocumentsStorage.Get(context, Constants.Documents.Replication.ReplicationConfigurationDocument);
                ReplicationDocument replicationDoc = null;

                if (configurationDocument != null)
                {
                    using (configurationDocument.Data)
                    {
                        replicationDoc = JsonDeserializationServer.ReplicationDocument(configurationDocument.Data);
                    }
                }
                if (replicationDoc == null)
                {
                    replicationDoc = new ReplicationDocument
                    {
                        DefaultResolver = new DatabaseResolver
                        {
                            ResolvingDatabaseId = uid,
                            Version = version.Value
                        }
                    };
                }
                else
                {
                    if (replicationDoc.DefaultResolver == null)
                    {
                        replicationDoc.DefaultResolver = new DatabaseResolver();
                    }

                    if (replicationDoc.DefaultResolver.Version >= version)
                        return;

                    replicationDoc.DefaultResolver.Version = version.Value;
                    replicationDoc.DefaultResolver.ResolvingDatabaseId = uid;
                }

                if (replicationDoc.DefaultResolver.Version == version &&
                    replicationDoc.DefaultResolver.ResolvingDatabaseId != uid)
                    ThrowConflictingResolvers(uid, version, replicationDoc.DefaultResolver.ResolvingDatabaseId);

                var djv = replicationDoc.ToJson();
                var replicatedBlittable = context.ReadObject(djv, Constants.Documents.Replication.ReplicationConfigurationDocument);

                Database.DocumentsStorage.Put(context, Constants.Documents.Replication.ReplicationConfigurationDocument, null, replicatedBlittable);

                context.Transaction.Commit();// will force reload of all connections as side affect
            }
            ReplicationDocument = GetReplicationDocument();
        }

        private void ThrowConflictingResolvers(string uid, int? version, string existingResolverDbId)
        {
            throw new InvalidOperationException(
                $"Resolver versions are conflicted. Same version {version}, but different database are set " +
                $"{uid} and {existingResolverDbId} as resovlers. " +
                "Increment the version of the preferred database resolver.");
        }


        private ReplicationDocument GetReplicationDocument()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var configurationDocument = Database.DocumentsStorage.Get(context, Constants.Documents.Replication.ReplicationConfigurationDocument);

                if (configurationDocument == null)
                    return null;

                using (configurationDocument.Data)
                {
                    return JsonDeserializationServer.ReplicationDocument(configurationDocument.Data);
                }
            }
        }

        public void Dispose()
        {
            var ea = new ExceptionAggregator("Failed during dispose of document replication loader");

            ea.Execute(_reconnectAttemptTimer.Dispose);

            Database.Changes.OnSystemDocumentChange -= OnSystemDocumentChange;

            ea.Execute(() => ConflictResolver.ResolveConflictsTask.Wait());

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

            var replicationDocument = ReplicationDocument;//thread safe copy
            if (replicationDocument?.Destinations == null)
                return result;
            ReplicationDestination disabledReplicationDestination = null;
            bool hasDisabled = false;
            foreach (var replicationDocumentDestination in replicationDocument.Destinations)
            {
                if (replicationDocumentDestination.Disabled)
                {
                    disabledReplicationDestination = replicationDocumentDestination;
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
                        $"The disabled replication destination {disabledReplicationDestination.Database} on " +
                        $"{disabledReplicationDestination.Url} prevents from cleaning large number of tombstones.",

                    type: PerformanceHintType.Replication,
                    notificationSeverity: NotificationSeverity.Warning,
                    source: $"{disabledReplicationDestination.Database} on {disabledReplicationDestination.Url}"
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

            public ReplicationDestination Destination { get; set; }

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
