using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class DocumentReplicationLoader : IDisposable
    {
        private readonly DocumentDatabase _database;
        private volatile bool _isInitialized;

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly Timer _reconnectAttemptTimer;
        private readonly ConcurrentSet<OutgoingReplicationHandler> _outgoing = new ConcurrentSet<OutgoingReplicationHandler>();
        private readonly ConcurrentDictionary<ReplicationDestination, ConnectionShutdownInfo> _outgoingFailureInfo = new ConcurrentDictionary<ReplicationDestination, ConnectionShutdownInfo>();

        private readonly ConcurrentDictionary<string, IncomingReplicationHandler> _incoming = new ConcurrentDictionary<string, IncomingReplicationHandler>();
        private readonly ConcurrentDictionary<IncomingConnectionInfo, DateTime> _incomingLastActivityTime = new ConcurrentDictionary<IncomingConnectionInfo, DateTime>();
        private readonly ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> _incomingRejectionStats = new ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>();

        private readonly ConcurrentSet<ConnectionShutdownInfo> _reconnectQueue = new ConcurrentSet<ConnectionShutdownInfo>();
        internal Dictionary<string, ScriptResolver> ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
        private readonly Logger _log;
        private ReplicationDocument _replicationDocument;
        private int _numberOfSiblings;

        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Values.Select(x => x.ConnectionInfo);
        public IEnumerable<ReplicationDestination> OutgoingConnections => _outgoing.Select(x => x.Destination);

        public IEnumerable<OutgoingReplicationHandler> OutgoingHandlers => _outgoing;
        public IEnumerable<IncomingReplicationHandler> IncomingHandlers => _incoming.Values;

        private readonly ConcurrentQueue<TaskCompletionSource<object>> _waitForReplicationTasks = new ConcurrentQueue<TaskCompletionSource<object>>();

        public DocumentReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _log = LoggingSource.Instance.GetLogger<DocumentReplicationLoader>(_database.Name);
            _reconnectAttemptTimer = new Timer(AttemptReconnectFailedOutgoing,
                null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
        }

        public IReadOnlyDictionary<ReplicationDestination, ConnectionShutdownInfo> OutgoingFailureInfo => _outgoingFailureInfo;
        public IReadOnlyDictionary<IncomingConnectionInfo, DateTime> IncomingLastActivityTime => _incomingLastActivityTime;
        public IReadOnlyDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> IncomingRejectionStats => _incomingRejectionStats;
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
            using(tcpConnectionOptions.ContextPool.AllocateOperationContext(out context))
            using (var readerObject = context.ParseToMemory(
                tcpConnectionOptions.Stream,
                "IncomingReplication/get-last-etag-message read",
                BlittableJsonDocumentBuilder.UsageMode.None, 
                tcpConnectionOptions.PinnedBuffer))
            {
                getLatestEtagMessage = JsonDeserializationServer.ReplicationLatestEtagRequest(readerObject);
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"GetLastEtag: {getLatestEtagMessage.SourceMachineName} / {getLatestEtagMessage.SourceDatabaseName} ({getLatestEtagMessage.SourceDatabaseId}) - {getLatestEtagMessage.SourceUrl}");
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
                DocumentsOperationContext documentsOperationContext;
                TransactionOperationContext configurationContext;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsOperationContext))
                using (_database.ConfigurationStorage.ContextPool.AllocateOperationContext(out configurationContext))
                using (var writer = new BlittableJsonTextWriter(documentsOperationContext, tcpConnectionOptions.Stream))
                using (var docTx = documentsOperationContext.OpenReadTransaction())
                using (var configTx = configurationContext.OpenReadTransaction())
                {
                    var documentsChangeVector = new DynamicJsonArray();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(documentsOperationContext))
                    {
                        documentsChangeVector.Add(new DynamicJsonValue
                        {
                            [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                            [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                        });
                    }

                    var indexesChangeVector = new DynamicJsonArray();
                    var changeVectorAsArray = _database.IndexMetadataPersistence.GetIndexesAndTransformersChangeVector(configTx.InnerTransaction);
                    foreach (var changeVectorEntry in changeVectorAsArray)
                    {
                        indexesChangeVector.Add(new DynamicJsonValue
                        {
                            [nameof(ChangeVectorEntry.DbId)] = changeVectorEntry.DbId.ToString(),
                            [nameof(ChangeVectorEntry.Etag)] = changeVectorEntry.Etag
                        });
                    }

                    var lastEtagFromSrc = _database.DocumentsStorage.GetLastReplicateEtagFrom(documentsOperationContext, getLatestEtagMessage.SourceDatabaseId);
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"GetLastEtag response, last etag: {lastEtagFromSrc}");
                    }
                    documentsOperationContext.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ReplicationMessageReply.Type)] = "Ok",
                        [nameof(ReplicationMessageReply.MessageType)] = ReplicationMessageType.Heartbeat,
                        [nameof(ReplicationMessageReply.LastEtagAccepted)] = lastEtagFromSrc,
                        [nameof(ReplicationMessageReply.LastIndexTransformerEtagAccepted)] = _database.IndexMetadataPersistence.GetLastReplicateEtagFrom(configTx.InnerTransaction, getLatestEtagMessage.SourceDatabaseId),
                        [nameof(ReplicationMessageReply.DocumentsChangeVector)] = documentsChangeVector,
                        [nameof(ReplicationMessageReply.IndexTransformerChangeVector)] = indexesChangeVector
                    });
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
                _log.Info($"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}", null);

            // need to safeguard against two concurrent connection attempts
            var newConnection = _incoming.GetOrAdd(newIncoming.ConnectionInfo.SourceDatabaseId, newIncoming);
            if (newConnection == newIncoming)
                newIncoming.Start();
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
                throw new InvalidOperationException($"Failed to parse source database Id. What I got is {(string.IsNullOrWhiteSpace(connectionInfo.SourceDatabaseId) ? "<empty string>" : _database.DbId.ToString())}. This is not supposed to happen and is likely a bug.");
            }

            if (sourceDbId == _database.DbId)
            {
                throw new InvalidOperationException($"Cannot have have replication with source and destination being the same database. They share the same db id ({connectionInfo} - {_database.DbId})");
            }

            IncomingReplicationHandler value;
            if (_incoming.TryRemove(connectionInfo.SourceDatabaseId, out value))
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Disconnecting existing connection from {value.FromToString} because we got a new connection from the same source db");
                }
                value.Dispose();
            }


        }

        public void Initialize()
        {
            if (_isInitialized) //precaution -> probably not necessary, but still...
                return;

            _isInitialized = true;

            _database.Notifications.OnSystemDocumentChange += OnSystemDocumentChange;

            InitializeOutgoingReplications();
            InitializeResolvers();
        }

        private void InitializeResolvers()
        {
            if (_replicationDocument?.ResolveByCollection == null)
            {
                if (ScriptConflictResolversCache.Count > 0)
                    ScriptConflictResolversCache = new Dictionary<string, ScriptResolver>();
                return;
            }
            var copy = new Dictionary<string, ScriptResolver>();
            foreach (var kvp in _replicationDocument.ResolveByCollection)
            {
                var collection = kvp.Key;
                var script = kvp.Value.Script;
                if (string.IsNullOrEmpty(script.Trim()))
                {
                    continue;
                }
                copy[collection] = new ScriptResolver
                {
                    Script = script
                };
            }
            ScriptConflictResolversCache = copy;
        }

        private void InitializeOutgoingReplications()
        {
            _replicationDocument = GetReplicationDocument();
            if (_replicationDocument?.Destinations == null || //precaution
                _replicationDocument.Destinations.Count == 0)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Tried to initialize outgoing replications, but there is no replication document or destinations are empty. Nothing to do...");

                _numberOfSiblings = 0;

                return;
            }

            if (_log.IsInfoEnabled)
                _log.Info($"Initializing {_replicationDocument.Destinations.Count:#,#} outgoing replications..");

            var countOfDestinations = 0;
            foreach (var destination in _replicationDocument.Destinations)
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

        private void AddAndStartOutgoingReplication(ReplicationDestination destination)
        {
            var outgoingReplication = new OutgoingReplicationHandler(_database, destination);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            _outgoing.TryAdd(outgoingReplication); // can't fail, this is a brand new instance
            _outgoingFailureInfo.TryAdd(destination, new ConnectionShutdownInfo
            {
                Destination = destination
            });

            outgoingReplication.SetMinimalHeartbeat(_replicationDocument.HeartbeatInterval);
            outgoingReplication.Start();
        }

        private void OnIncomingReceiveFailed(IncomingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                IncomingReplicationHandler _;
                _incoming.TryRemove(instance.ConnectionInfo.SourceDatabaseId, out _);

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

                ConnectionShutdownInfo failureInfo;
                if (_outgoingFailureInfo.TryGetValue(instance.Destination, out failureInfo) == false)
                    return;

                failureInfo.OnError(e);
                failureInfo.DestinationDbId = instance.DestinationDbId;
                failureInfo.LastHeartbeatTicks = instance.LastHeartbeatTicks;
                failureInfo.LastAcceptedDocumentEtag = instance.LastAcceptedDocumentEtag;
                failureInfo.LastSentIndexOrTransformerEtag = instance._lastSentIndexOrTransformerEtag;

                _reconnectQueue.Add(failureInfo);

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Destination}) failed, and the connection will be retried later.",
                        e);
            }
        }

        private void OnOutgoingSendingSucceeded(OutgoingReplicationHandler instance)
        {
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

        private void OnSystemDocumentChange(DocumentChangeNotification notification)
        {

            if (!notification.Key.Equals(Constants.Replication.DocumentReplicationConfiguration, StringComparison.OrdinalIgnoreCase))
                return;

            if (_log.IsInfoEnabled)
                _log.Info("System document change detected. Starting and stopping outgoing replication threads.");

            //prevent reconnecting to a destination that we shouldn't in case we have flaky network
            _reconnectQueue.Clear();

            foreach (var instance in _outgoing)
            {
                instance.Failed -= OnOutgoingSendingFailed;
                instance.SuccessfulTwoWaysCommunication -= OnOutgoingSendingSucceeded;
                instance.Dispose();
            }

            _outgoing.Clear();
            _outgoingFailureInfo.Clear();

            InitializeOutgoingReplications();

            InitializeResolvers();

            if (_log.IsInfoEnabled)
                _log.Info($"Replication configuration was changed: {notification.Key}");
        }

        internal ReplicationDocument GetReplicationDocument()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var configurationDocument = _database.DocumentsStorage.Get(context, Constants.Replication.DocumentReplicationConfiguration);

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
            _cts.Cancel();
            _reconnectAttemptTimer.Dispose();

            _database.Notifications.OnSystemDocumentChange -= OnSystemDocumentChange;

            if (_log.IsInfoEnabled)
                _log.Info("Closing and disposing document replication connections.");

            foreach (var incoming in _incoming)
                incoming.Value.Dispose();

            foreach (var outgoing in _outgoing)
                outgoing.Dispose();

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
    }
}
