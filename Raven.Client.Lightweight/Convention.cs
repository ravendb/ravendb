using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace Raven.Client
{
	public abstract class Convention : QueryConvention
	{
		protected Dictionary<Type, MemberInfo> idPropertyCache = new Dictionary<Type, MemberInfo>();

		/// <summary>
		/// Gets or sets the function to find the identity property.
		/// </summary>
		/// <value>The find identity property.</value>
		public Func<MemberInfo, bool> FindIdentityProperty { get; set; }

		/// <summary>
		/// Gets the identity property.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public MemberInfo GetIdentityProperty(Type type)
		{
			MemberInfo info;
			var currentIdPropertyCache = idPropertyCache;
			if (currentIdPropertyCache.TryGetValue(type, out info))
				return info;

			var identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);

			if (identityProperty != null && identityProperty.DeclaringType != type)
			{
				var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
				identityProperty = propertyInfo ?? identityProperty;
			}

			idPropertyCache = new Dictionary<Type, MemberInfo>(currentIdPropertyCache)
			{
				{type, identityProperty}
			};

			return identityProperty;
		}

		private static IEnumerable<MemberInfo> GetPropertiesForType(Type type)
		{
			foreach (var propertyInfo in ReflectionUtil.GetPropertiesAndFieldsFor(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
			{
				yield return propertyInfo;
			}

			foreach (var @interface in type.GetInterfaces())
			{
				foreach (var propertyInfo in GetPropertiesForType(@interface))
				{
					yield return propertyInfo;
				}
			}
		}
	}
}