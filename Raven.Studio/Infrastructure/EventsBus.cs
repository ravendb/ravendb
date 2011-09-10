using System;
using System.Collections.Generic;

namespace Raven.Studio.Infrastructure
{
	public class EventsBus
	{
		private static readonly Dictionary<Type, List<Func<object, bool>>> subscribiers = new Dictionary<Type, List<Func<object, bool>>>();

		public static void Subscribe<T>(Action<T> action)
		{
			lock (subscribiers)
			{
				List<Func<object, bool>> list;
				if (subscribiers.TryGetValue(typeof(T), out list) == false)
					subscribiers.Add(typeof(T), list = new List<Func<object, bool>>());

				var reference = new WeakReference(action.ViaCurrentDispatcher());
				list.Add(o =>
				{
					var act = reference.Target as Action<T>;
					if (act == null)
						return false;

					act((T) o);
					return true;
				});
			}
		}

		public static void Notify<T>(T msg)
		{
			lock (subscribiers)
			{
				List<Func<object, bool>> list;
				if (subscribiers.TryGetValue(typeof(T), out list) == false)
					return;

				foreach (var action in list.ToArray())
				{
					if (action(msg) == false)
						list.Remove(action);
				}
			}
		}
	}
}