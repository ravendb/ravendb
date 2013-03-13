using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.UniqueConstraints
{
	using System.Collections.Concurrent;
	using System.Reflection;

	public static class UniqueConstraintsTypeDictionary
	{
		public static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertiesCache = new ConcurrentDictionary<Type, PropertyInfo[]>();

		public static PropertyInfo[] GetProperties(Type t)
		{
			return PropertiesCache.GetOrAdd(t,
			                                type =>
					                                type.GetProperties()
														.Where(p => Attribute.IsDefined(p, typeof (UniqueConstraintAttribute)))
														.ToArray()
				                                );
		}
	}
}
