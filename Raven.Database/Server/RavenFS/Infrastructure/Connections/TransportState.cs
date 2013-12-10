using System;
using System.Collections.Concurrent;
using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Infrastructure.Connections
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
			if (connections.TryRemove(id, out value))
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


		public void Send(Notification notification)
		{
			OnNotification(this, notification);
			foreach (var connectionState in connections)
			{
				connectionState.Value.Send(notification);
			}
		}

		public event Action<object, Notification> OnNotification = delegate { };

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
