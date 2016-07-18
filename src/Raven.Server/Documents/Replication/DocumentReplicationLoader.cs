using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
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
		private readonly ConcurrentDictionary<ReplicationDestination,OutgoingReplicationHandler> _outgoing = new ConcurrentDictionary<ReplicationDestination, OutgoingReplicationHandler>();
		private readonly ConcurrentDictionary<ReplicationDestination, ConnectionFailureInfo> _outgoingFailureInfo = new ConcurrentDictionary<ReplicationDestination, ConnectionFailureInfo>();

		private readonly ConcurrentSet<Lazy<IncomingReplicationHandler>> _incoming = new ConcurrentSet<Lazy<IncomingReplicationHandler>>();
		private readonly ConcurrentDictionary<IncomingConnectionInfo, DateTime> _incomingLastActivityTime = new ConcurrentDictionary<IncomingConnectionInfo, DateTime>();
		private readonly ConcurrentDictionary<IncomingConnectionInfo,ConcurrentQueue<IncomingConnectionRejectionInfo>> _incomingRejectionStats = new ConcurrentDictionary<IncomingConnectionInfo, ConcurrentQueue<IncomingConnectionRejectionInfo>>();

		private readonly ConcurrentQueue<ReplicationDestination> _reconnectQueue = new ConcurrentQueue<ReplicationDestination>();

		private readonly Logger _log;

		public DocumentReplicationLoader(DocumentDatabase database)
		{
			_database = database;
			_log = _database.LoggerSetup.GetLogger<DocumentReplicationLoader>(_database.Name);
			_reconnectAttemptTimer = new Timer(AttemptReconnectFailedOutgoing,
				null, TimeSpan.Zero, TimeSpan.FromMilliseconds(45000));
		}

		public void AcceptIncomingConnection(TcpConnectionHeaderMessage incomingMessageHeader,
			JsonOperationContext.MultiDocumentParser multiDocumentParser,
			NetworkStream stream)
		{
			ReplicationLatestEtag getLatestEtagMessage;
			using (var readerObject = multiDocumentParser.ParseToMemory("IncomingReplication/get-last-etag-message read"))
			{
				getLatestEtagMessage = JsonDeserialization.ReplicationLatestEtag(readerObject);
			}

			DocumentsOperationContext context;
			using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
			using (var writer = new BlittableJsonTextWriter(context, stream))
			using (context.OpenReadTransaction())
			{
				context.Write(writer, new DynamicJsonValue
				{
					["LastSentEtag"] = GetLastReceivedEtag(Guid.Parse(getLatestEtagMessage.SourceDatabaseId), context)
				});
			}
		
			var connectionInfo = IncomingConnectionInfo.FromGetLatestEtag(getLatestEtagMessage);

			string rejectionMessage;
			if (!IsValidConnection(connectionInfo, incomingMessageHeader, out rejectionMessage))
			{
				if (_log.IsInfoEnabled)
					_log.Info($"Connection from [{connectionInfo}] is rejected. Reason: {rejectionMessage}");

				_incomingRejectionStats.AddOrUpdate(connectionInfo,
					_ => 
					{
						var queue = new ConcurrentQueue<IncomingConnectionRejectionInfo>();
						queue.Enqueue(new IncomingConnectionRejectionInfo { Reason = rejectionMessage });
						return queue;
					},
					(_, existing) =>
					{
						existing.Enqueue(new IncomingConnectionRejectionInfo { Reason = rejectionMessage });
						return existing;
					});

				return;
			}

			var lazyIncomingHandler = new Lazy<IncomingReplicationHandler>(() =>
			{
				var newIncoming = new IncomingReplicationHandler(multiDocumentParser,
						_database,
						stream,
						getLatestEtagMessage);
				newIncoming.Failed += OnIncomingReceiveFailed;
				newIncoming.DocumentsReceived += OnIncomingReceiveSucceeded;
				if (_log.IsInfoEnabled)
					_log.Info($"Initialized document replication connection from {connectionInfo.SourceDatabaseName} located at {connectionInfo.SourceUrl}", null);
				return newIncoming;
			});

			_incoming.Add(lazyIncomingHandler);

			lazyIncomingHandler.Value.Start();			

		}

		private void AttemptReconnectFailedOutgoing(object state)
		{
			ReplicationDestination destination;
			while (_reconnectQueue.TryDequeue(out destination))
			{
				_cts.Token.ThrowIfCancellationRequested();
				AddAndStartOutgoingReplication(destination);
			}
		}

		private bool IsValidConnection(IncomingConnectionInfo connectionInfo, TcpConnectionHeaderMessage incomingMessageHeader, out string rejectionMessage)
		{
			rejectionMessage = null;
			
			//not 100% sure it is enough for detecting loopback replication
			if (incomingMessageHeader.DatabaseName.Equals(connectionInfo.SourceDatabaseName) &&
				connectionInfo.SourceMachineName.Equals(Environment.MachineName))
			{
				rejectionMessage = $"Cannot have have replication with source and destination being the same database. ({connectionInfo})";
				return false;
			}

			var relevantActivityEntry =
				_incomingLastActivityTime.FirstOrDefault(x => x.Key.SourceDatabaseId.Equals(connectionInfo.SourceDatabaseId,StringComparison.OrdinalIgnoreCase));

			if (relevantActivityEntry.Key != null &&
				(relevantActivityEntry.Value - DateTime.UtcNow).TotalMilliseconds <= 
				_database.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds)
			{
				rejectionMessage = $"Tried to connect [{connectionInfo}], but the connection from the same source was active less then 30 seconds ago. Duplicate connections from the same source are not allowed.";
				return false;
			}

			return true;
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

			foreach (var destination in replicationDocument.Destinations)
			{
				AddAndStartOutgoingReplication(destination);
			}
		}

		private void AddAndStartOutgoingReplication(ReplicationDestination destination)
		{
			var outgoingReplication = new OutgoingReplicationHandler(_database, destination);
			outgoingReplication.Failed += OnOutgoingSendingFailed;
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
				_outgoingFailureInfo.TryAdd(destination, new ConnectionFailureInfo());
				outgoingReplication.Start();
			}
		}

		private void OnIncomingReceiveFailed(IncomingReplicationHandler instance, Exception e)
		{
			using (instance)
			{
				IncomingReplicationHandler _;
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
				instance.DocumentsSent -= OnOutgoingSendingSucceeded;
				instance.Failed -= OnOutgoingSendingFailed;
				OutgoingReplicationHandler _;
				_outgoing.TryRemove(instance.Destination, out _);

				var failureInfo = _outgoingFailureInfo[instance.Destination];
				failureInfo.OnError();
						
				_reconnectQueue.Enqueue(instance.Destination);

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

		private long GetLastReceivedEtag(Guid srcDbId, DocumentsOperationContext context)
		{
			var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
			var vectorEntry = dbChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
			return vectorEntry.Etag;
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
					return JsonDeserialization.ReplicationDocument(configurationDocument.Data);
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
			foreach(var incoming in _incoming)
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

			public void Reset()
			{
				NextTimout = TimeSpan.FromMilliseconds(500);
				ErrorCount = 0;
			}

			public void OnError()
			{
				ErrorCount++;
				NextTimout = TimeSpan.FromMilliseconds(Math.Min(NextTimout.TotalMilliseconds * 4, MaxConnectionTimout));
			}
		}
	}
}
