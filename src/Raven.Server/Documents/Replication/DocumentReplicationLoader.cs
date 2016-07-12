using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
	//TODO : add code to create outgoing connections
	public class DocumentReplicationLoader : IDisposable
	{
		private readonly DocumentDatabase _database;

		private readonly ConcurrentDictionary<ReplicationDestination,OutgoingReplicationHandler> _outgoing = new ConcurrentDictionary<ReplicationDestination, OutgoingReplicationHandler>();
		private readonly ConcurrentDictionary<IncomingConnectionInfo, IncomingReplicationHandler> _incoming = new ConcurrentDictionary<IncomingConnectionInfo, IncomingReplicationHandler>();
		private readonly Logger _log;

		public DocumentReplicationLoader(DocumentDatabase database)
		{
			_database = database;
			_log = _database.LoggerSetup.GetLogger<DocumentReplicationLoader>(_database.Name);
		}

		public void AcceptIncomingConnection(TcpConnectionHeaderMessage incomingMessageHeader, 
			JsonOperationContext.MultiDocumentParser multiDocumentParser, 
			NetworkStream stream)
		{
			//TODO : revise accepting new connection workflow; maybe old connection should be disposed?
			_incoming.GetOrAdd(IncomingConnectionInfo.FromIncomingHeader(incomingMessageHeader), 
				key =>
				{
					var newIncoming = new IncomingReplicationHandler(key, incomingMessageHeader, multiDocumentParser, _database, stream);
					newIncoming.Failed += IncomingReplicationHandlerFailed;
					newIncoming.Start();					
					_log.InfoIfEnabled($"Initialized document replication connection with {key.SourceDatabaseName} from {key.SourceUrl}");
					return newIncoming;
				});
		}

		private void IncomingReplicationHandlerFailed(Exception exception, IncomingReplicationHandler incomingReplicationHandler)
		{
			incomingReplicationHandler.Failed -= IncomingReplicationHandlerFailed;
			_log.OperaitonsIfEnabled($"Incoming replication handler has thrown an unhandled exception. ({incomingReplicationHandler.FromToString})",exception);
			incomingReplicationHandler.Dispose();
		}

		public void Dispose()
		{
			_log.InfoIfEnabled("Closing and disposing document replication connections.");
			foreach(var incoming in _incoming)
				incoming.Value.Dispose();

			foreach (var outgoing in _outgoing)
				outgoing.Value.Dispose();
		}
	}
}
