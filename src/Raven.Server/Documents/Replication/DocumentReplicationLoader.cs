using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Server.Documents.Replication
{
	public class DocumentReplicationLoader : IDisposable
	{
		private readonly DocumentDatabase _database;

		private readonly ConcurrentDictionary<ReplicationDestination,OutgoingReplicationHandler> _outgoing = new ConcurrentDictionary<ReplicationDestination, OutgoingReplicationHandler>();
		private readonly ConcurrentDictionary<TcpConnectionHeaderMessage, IncomingReplicationHandler> _incoming = new ConcurrentDictionary<TcpConnectionHeaderMessage, IncomingReplicationHandler>();

		public DocumentReplicationLoader(DocumentDatabase database)
		{
			_database = database;
		}

		public void AcceptIncomingConnection(TcpConnectionHeaderMessage incomingMessageHeader, NetworkStream tcpStream)
		{
			_incoming.AddOrUpdate(incomingMessageHeader,
				key =>
				{
					var newIncoming = new IncomingReplicationHandler(key, tcpStream, _database);
					newIncoming.Start();
					return newIncoming;
				},
				(key, existing) =>
				{
					existing.Dispose();
					var newIncoming = new IncomingReplicationHandler(key, tcpStream, _database);
					newIncoming.Start();
					return newIncoming;
				});
		}
	
		public void Dispose()
		{
			foreach(var incoming in _incoming)
				incoming.Value.Dispose();

			foreach (var outgoing in _outgoing)
				outgoing.Value.Dispose();
		}
	}
}
