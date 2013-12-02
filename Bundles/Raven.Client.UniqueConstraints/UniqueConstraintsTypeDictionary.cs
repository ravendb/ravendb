using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.UniqueConstraints
{
	using System.Collections.Concurrent;
	using System.Reflection;

	public class UniqueConstraintsTypeDictionary
	{
		public readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertiesCache = new ConcurrentDictionary<Type, PropertyInfo[]>();

		public PropertyInfo[] GetProperties(Type t)
		{
			if (t == null) { return new PropertyInfo[0]; }

			return PropertiesCache.GetOrAdd(t, GetUniqueProperties);
		}

		protected virtual PropertyInfo[] GetUniqueProperties(Type type)
		{
			return type.GetProperties()
				.Where(p => Attribute.IsDefined(p, typeof(UniqueConstraintAttribute)))
				.ToArray();
		}

		public static UniqueConstraintsTypeDictionary FindDictionary(IDocumentStore store)
		{
			return InternalFindDictionary(store as DocumentStoreBase);
		}

		private static UniqueConstraintsTypeDictionary InternalFindDictionary(DocumentStoreBase store)
		{
			if (store != null)
			{
				try
				{
					UniqueConstraintsStoreListener listener =
						store.RegisteredStoreListeners.OfType<UniqueConstraintsStoreListener>().SingleOrDefault();

					if (listener != null)
					{
						return listener.UniqueConstraintsTypeDictionary;
					}
				}
				catch (InvalidOperationException)
				{
					// Multiple dictionaries found; this should never happen.
				}
			}

			return null;
		}
	}
}
