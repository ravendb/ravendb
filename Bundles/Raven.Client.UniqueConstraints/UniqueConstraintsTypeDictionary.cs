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
		public readonly ConcurrentDictionary<Type, ConstraintInfo[]> PropertiesCache = new ConcurrentDictionary<Type, ConstraintInfo[]>();

		public ConstraintInfo[] GetProperties(Type t)
		{
			if (t == null) { return new ConstraintInfo[0]; }

			return PropertiesCache.GetOrAdd(t, GetUniqueProperties);
		}

		protected virtual ConstraintInfo[] GetUniqueProperties(Type type)
		{
			var attrType = typeof(UniqueConstraintAttribute);
			return type.GetProperties()
				.Where(p => Attribute.IsDefined(p, attrType))
				.Select(pi => new ReflectedConstraintInfo(pi, (UniqueConstraintAttribute)Attribute.GetCustomAttribute(pi, attrType)))
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
