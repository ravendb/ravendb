// -----------------------------------------------------------------------
//  <copyright file="TransportState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Connections
{
	public class TransportState
	{
		readonly TimeSensitiveStore<string> timeSensitiveStore = new TimeSensitiveStore<string>(TimeSpan.FromSeconds(45));

		readonly ConcurrentDictionary<string, ConnectionState> connections = new ConcurrentDictionary<string, ConnectionState>();

		public TimeSensitiveStore<string> TimeSensitiveStore
		{
			get { return timeSensitiveStore; }
		}

		public void OnIdle()
		{
			ConnectionState _;
			timeSensitiveStore.ForAllExpired(s => connections.TryRemove(s, out _));
		}

		public void Disconnect(string id)
		{
			timeSensitiveStore.Seen(id);
			ConnectionState value;
			if(connections.TryRemove(id, out value))
				value.Disconnect();
		}

		public ConnectionState Register(EventsTransport transport)
		{
			timeSensitiveStore.Seen(transport.Id);
			transport.Disconnected += () => TimeSensitiveStore.Missing(transport.Id);
			return connections.AddOrUpdate(transport.Id, new ConnectionState(transport), (s, state) =>
			                                                                             	{
			                                                                             		state.Reconnect(transport);
			                                                                             		return state;
			                                                                             	});
		}

		public event Action<object, IndexChangeNotification> OnIndexChangeNotification = delegate { }; 

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			OnIndexChangeNotification(this, indexChangeNotification);
			foreach (var connectionState in connections)
			{
				connectionState.Value.Send(indexChangeNotification);
			}
		}

		public event Action<object, DocumentChangeNotification> OnDocumentChangeNotification = delegate { }; 

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			OnDocumentChangeNotification(this, documentChangeNotification);
			foreach (var connectionState in connections)
			{
				connectionState.Value.Send(documentChangeNotification);
			}
		}

		public ConnectionState For(string id)
		{
			return connections.GetOrAdd(id, _ =>
			                                	{
			                                		var connectionState = new ConnectionState(null);
			                                		TimeSensitiveStore.Missing(id);
			                                		return connectionState;
			                                	});
		}
	}
}