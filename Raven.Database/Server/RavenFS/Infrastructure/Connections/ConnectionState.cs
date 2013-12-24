using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Util;

namespace Raven.Database.Server.RavenFS.Infrastructure.Connections
{
	public class ConnectionState
	{
		private readonly ConcurrentSet<string> matchingFolders =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentQueue<Notification> pendingMessages = new ConcurrentQueue<Notification>();

		private EventsTransport eventsTransport;
		private int watchCancellations;
		private int watchConfig;
		private int watchConflicts;
		private int watchSync;

		public ConnectionState(EventsTransport eventsTransport)
		{
			this.eventsTransport = eventsTransport;
		}

		public async Task Send(Notification notification)
		{
			if (ShouldSend(notification))
				await Enqueue(notification);
		}

		private bool ShouldSend(Notification notification)
		{
			if (notification is FileChange &&
				matchingFolders.Any(
					f => ((FileChange)notification).File.StartsWith(f, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			if (notification is ConfigChange && watchConfig > 0)
			{
				return true;
			}

			if (notification is ConflictNotification && watchConflicts > 0)
			{
				return true;
			}

			if (notification is SynchronizationUpdate && watchSync > 0)
			{
				return true;
			}

			if (notification is UploadFailed && watchCancellations > 0)
			{
				return true;
			}

			return false;
		}

		private async Task Enqueue(Notification msg)
		{
			if (eventsTransport == null || eventsTransport.Connected == false)
			{
				pendingMessages.Enqueue(msg);
				return;
			}
			try
			{
				await eventsTransport.SendAsync(msg);
				pendingMessages.Enqueue(msg);
			}
			catch (Exception)
			{
			}
		}

		public async Task Reconnect(EventsTransport transport)
		{
			eventsTransport = transport;
			var items = new List<Notification>();
			Notification result;
			while (pendingMessages.TryDequeue(out result))
			{
				items.Add(result);
			}

			try
			{
				await eventsTransport.SendManyAsync(items);
				foreach (var item in items)
				{
					pendingMessages.Enqueue(item);
				}
			}
			catch (Exception)
			{
			}
		}

		public void WatchConflicts()
		{
			Interlocked.Increment(ref watchConflicts);
		}

		public void UnwatchConflicts()
		{
			Interlocked.Decrement(ref watchConflicts);
		}

		public void WatchConfig()
		{
			Interlocked.Increment(ref watchConfig);
		}

		public void UnwatchConfig()
		{
			Interlocked.Decrement(ref watchConfig);
		}

		public void WatchSync()
		{
			Interlocked.Increment(ref watchSync);
		}

		public void UnwatchSync()
		{
			Interlocked.Decrement(ref watchSync);
		}

		public void WatchFolder(string folder)
		{
			matchingFolders.TryAdd(folder);
		}

		public void UnwatchFolder(string folder)
		{
			matchingFolders.TryRemove(folder);
		}

		public void WatchCancellations()
		{
			Interlocked.Increment(ref watchCancellations);
		}

		public void UnwatchCancellations()
		{
			Interlocked.Decrement(ref watchCancellations);
		}

		public void Disconnect()
		{
			if (eventsTransport != null)
				eventsTransport.Disconnect();
		}
	}
}
