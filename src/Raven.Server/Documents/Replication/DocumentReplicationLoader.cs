using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
    //TODO : add code to create outgoing connections
    public class DocumentReplicationLoader : IDisposable
    {
        private readonly DocumentDatabase _database;
        private volatile bool _isInitialized;

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly Timer _reconnectAttemptTimer;
        private readonly ConcurrentDictionary<ReplicationDestination, OutgoingReplicationHandler> _outgoing = new ConcurrentDictionary<ReplicationDestination, OutgoingReplicationHandler>();
        private readonly ConcurrentDictionary<ReplicationDestination, ConnectionFailureInfo> _outgoingFailureInfo = new ConcurrentDictionary<ReplicationDestination, ConnectionFailureInfo>();

        private readonly ConcurrentSet<Lazy<IncomingReplicationHandler>> _incoming = new ConcurrentSet<Lazy<IncomingReplicationHandler>>();
        private readonly ConcurrentDictionary<IncomingConnectionInfo, DateTime> _incomingLastActivityTime = new ConcurrentDictionary<IncomingConnectionInfo, DateTime>();
        private readonly ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> _incomingRejectionStats = new ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>();

        private readonly ConcurrentSet<ConnectionFailureInfo> _reconnectQueue = new ConcurrentSet<ConnectionFailureInfo>();

        private readonly Logger _log;

        public IEnumerable<IncomingConnectionInfo> IncomingConnections => _incoming.Select(x => x.Value.ConnectionInfo);
        public IEnumerable<ReplicationDestination> OutgoingConnections => _outgoing.Keys;

        public DocumentReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _log = LoggerSetup.Instance.GetLogger<DocumentReplicationLoader>(_database.Name);
            _reconnectAttemptTimer = new Timer(AttemptReconnectFailedOutgoing,
                null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
        }

        public IReadOnlyDictionary<ReplicationDestination, ConnectionFailureInfo> OutgoingFailureInfo => _outgoingFailureInfo;
        public IReadOnlyDictionary<IncomingConnectionInfo, DateTime> IncomingLastActivityTime => _incomingLastActivityTime;
        public IReadOnlyDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>> IncomingRejectionStats => _incomingRejectionStats;
        public IEnumerable<ReplicationDestination> ReconnectQueue => _reconnectQueue.Select(x=>x.Destination);

        public void AcceptIncomingConnection(TcpConnectionOptions tcpConnectionOptions)
        {
            ReplicationLatestEtagRequest getLatestEtagMessage;
            using (var readerObject = tcpConnectionOptions.MultiDocumentParser.ParseToMemory("IncomingReplication/get-last-etag-message read"))
            {
                getLatestEtagMessage = JsonDeserializationServer.ReplicationLatestEtagRequest(readerObject);
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

            DocumentsOperationContext documentsOperationContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsOperationContext))
            using (var writer = new BlittableJsonTextWriter(documentsOperationContext, tcpConnectionOptions.Stream))
            using (documentsOperationContext.OpenReadTransaction())
            {
                var changeVector = new DynamicJsonArray();
                foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(documentsOperationContext))
                {
                    changeVector.Add(new DynamicJsonValue
                    {
                        ["DbId"] = changeVectorEntry.DbId.ToString(),
                        ["Etag"] = changeVectorEntry.Etag
                    });
                }
                documentsOperationContext.Write(writer, new DynamicJsonValue
                {
                    ["Type"] = "Ok",
                    ["LastEtagAccepted"] = _database.DocumentsStorage.GetLastReplicateEtagFrom(documentsOperationContext, getLatestEtagMessage.SourceDatabaseId),
                    ["CurrentChangeVector"] = changeVector
                });
                writer.Flush();
            }
           

            var lazyIncomingHandler = new Lazy<IncomingReplicationHandler>(() =>
            {
                //TODO: fix the disposable of the passed context and all the params cleanly
	            var newIncoming = new IncomingReplicationHandler(tcpConnectionOptions.MultiDocumentParser, _database,
		            tcpConnectionOptions.TcpClient, tcpConnectionOptions.Stream, getLatestEtagMessage);
                newIncoming.Failed += OnIncomingReceiveFailed;
                newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
                if (_log.IsInfoEnabled)
                    _log.Info($"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}", null);
                return newIncoming;
            });

            _incoming.Add(lazyIncomingHandler);

            //TODO: Why are we using lazy here?
            lazyIncomingHandler.Value.Start();
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
                            _log.Info($"Failed to start outgoing replciation to {failure.Destination}", e);
                        }
                    }
                }
                else
                {
                    if (minDiff < diff)
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

            if (Guid.Parse(connectionInfo.SourceDatabaseId) == _database.DbId)
            {
                throw new InvalidOperationException(
                    "Cannot have have replication with source and destination being the same database. They share the same db id ({connectionInfo} - {_database.DbId})");
            }

            var relevantActivityEntry =
                _incomingLastActivityTime.FirstOrDefault(x => x.Key.SourceDatabaseId.Equals(connectionInfo.SourceDatabaseId, StringComparison.OrdinalIgnoreCase));

            if (relevantActivityEntry.Key != null &&
                (relevantActivityEntry.Value - DateTime.UtcNow).TotalMilliseconds <=
                _database.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds)
            {
                throw new InvalidOperationException(
                    $"Tried to connect [{connectionInfo}], but the connection from the same source was active less then {_database.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalSeconds} ago. Duplicate connections from the same source are not allowed.");
            }
        }

        public void Initialize()
        {
            if (_isInitialized) //precaution -> probably not necessary, but still...
                return;

            _isInitialized = true;

            _database.Notifications.OnSystemDocumentChange += OnSystemDocumentChange;

            InitializeOutgoingReplications();
        }

        private void InitializeOutgoingReplications()
        {
            var replicationDocument = GetReplicationDocument();
            if (replicationDocument?.Destinations == null) //precaution
                return;

			if (_log.IsInfoEnabled)
				_log.Info("Initializing outgoing replications..");
			foreach (var destination in replicationDocument.Destinations)
            {
				if(_log.IsInfoEnabled)
					_log.Info($"Initialized outgoing replication for [{destination.Database}/{destination.Url}]");
                AddAndStartOutgoingReplication(destination);
            }
			if (_log.IsInfoEnabled)
				_log.Info("Finished initialization of outgoing replications..");
		}

		private void AddAndStartOutgoingReplication(ReplicationDestination destination)
        {
            var outgoingReplication = new OutgoingReplicationHandler(_database, destination);
            outgoingReplication.Failed += OnOutgoingSendingFailed;
            outgoingReplication.SuccessfulTwoWaysCommunication += OnOutgoingSendingSucceeded;
            if (!_outgoing.TryAdd(destination, outgoingReplication))
            {
                //keep outgoing replication unique per url/database name?
                //this is reasonable I think, but not 100% sure
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Tried to add outgoing destination and failed. Do you have duplicate destinations? (the destination that could not add was -> {destination})");
                outgoingReplication.Dispose();
            }
            else
            {
                _outgoingFailureInfo.TryAdd(destination, new ConnectionFailureInfo
                {
                    Destination = destination
                });
                outgoingReplication.Start();
            }
        }

        private void OnIncomingReceiveFailed(IncomingReplicationHandler instance, Exception e)
        {
            using (instance)
            {
                var storedInstance = _incoming.FirstOrDefault(x => ReferenceEquals(x.Value, instance));
                if (storedInstance != null)
                    _incoming.TryRemove(storedInstance);

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
                OutgoingReplicationHandler _;
                _outgoing.TryRemove(instance.Destination, out _);

                ConnectionFailureInfo failureInfo;
                if (_outgoingFailureInfo.TryGetValue(instance.Destination, out failureInfo) == false)
                    return;

                _reconnectQueue.Add(failureInfo);

                if (_log.IsInfoEnabled)
                    _log.Info($"Document replication connection ({instance.Destination}) failed, and the connection will be retried later.",
                        e);
            }
        }

        private void OnOutgoingSendingSucceeded(OutgoingReplicationHandler instance)
        {
            ConnectionFailureInfo failureInfo;
            if (_outgoingFailureInfo.TryGetValue(instance.Destination, out failureInfo))
                failureInfo.Reset();
        }

        private void OnIncomingReceiveSucceeded(IncomingReplicationHandler instance)
        {
            _incomingLastActivityTime.AddOrUpdate(instance.ConnectionInfo, DateTime.UtcNow, (_, __) => DateTime.UtcNow);
        }

        private void OnSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (!notification.Key.Equals(Constants.Replication.DocumentReplicationConfiguration, StringComparison.OrdinalIgnoreCase))
                return;
            // TODO: logging
            var outgoing = _outgoing.ToList();
            _outgoing.Clear();

            foreach (var instance in outgoing)
                instance.Value.Dispose();
            _outgoingFailureInfo.Clear();

            InitializeOutgoingReplications();

            if (_log.IsInfoEnabled)
                _log.Info($"Replication configuration was changed: {notification.Key}");
        }

        private ReplicationDocument GetReplicationDocument()
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
                _log.Info("Closing and disposing document replication connections.", null);
            foreach (var incoming in _incoming)
                incoming.Value.Dispose();

            foreach (var outgoing in _outgoing)
                outgoing.Value.Dispose();

        }

        public class IncomingConnectionRejectionInfo
        {
            public string Reason { get; set; }
            public DateTime When { get; } = DateTime.UtcNow;
        }

        public class ConnectionFailureInfo
        {
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

            public void OnError()
            {
                ErrorCount++;
                NextTimout = TimeSpan.FromMilliseconds(Math.Min(NextTimout.TotalMilliseconds * 4, MaxConnectionTimout));
                RetryOn = DateTime.UtcNow + NextTimout;
            }
        }
    }
}
