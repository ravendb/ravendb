using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Raven.Database.Util
{
	public class WeakEvent<T> where T : EventArgs
	{
		private readonly ConcurrentDictionary<WeakReference, object> instances = new ConcurrentDictionary<WeakReference, object>();

		public void Subscribe(EventHandler<T> handler)
		{
			instances.TryAdd(new WeakReference(handler), null);
			Cleanup();
		}

		public void Invoke(object sender, T args)
		{
			foreach (var eventHandler in instances.Select(instance => instance.Key.Target).OfType<EventHandler<T>>())
			{
				eventHandler(sender, args);
			}
			Cleanup();
		}

		private void Cleanup()
		{
			foreach (var instance in instances.Where(instance => instance.Key.IsAlive == false))
			{
				object _;
				instances.TryRemove(instance.Key, out _);
			}
		}
	}
}