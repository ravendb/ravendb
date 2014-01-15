using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Raven.Database.Server.RavenFS.Infrastructure.Connections
{
	public class TimeSensitiveStore<T>
	{
		private readonly TimeSpan timeout;

		private readonly ConcurrentDictionary<T, DateTime> lastSeen = new ConcurrentDictionary<T, DateTime>();

		public TimeSensitiveStore(TimeSpan timeout)
		{
			this.timeout = timeout;
		}

		public void Seen(T item)
		{
			DateTime time;
			lastSeen.TryRemove(item, out time);
		}

		public void Missing(T item)
		{
			var now = DateTime.UtcNow;
			lastSeen.AddOrUpdate(item, now, (arg1, time) => now);
		}

		public void ForAllExpired(Action<T> action)
		{
			var now = DateTime.UtcNow;
			foreach (var kvp in from kvp in lastSeen
								let durationNotSeen = now - kvp.Value
								where durationNotSeen >= timeout
								select kvp)
			{
				DateTime currentTime;
				if (lastSeen.TryRemove(kvp.Key, out currentTime) == false)
					continue;

				if (kvp.Value != currentTime) // maybe already seen?
				{
					lastSeen.TryAdd(kvp.Key, currentTime); // restore and continue
					continue;
				}
				action(kvp.Key);// removed, notify about this
			}
		}
	}
}