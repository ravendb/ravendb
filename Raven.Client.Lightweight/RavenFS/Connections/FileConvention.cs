using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Client.Document;

namespace Raven.Client.RavenFS.Connections
{
	/// <summary>
	/// The set of conventions used by the <see cref="FileConvention"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class FileConvention
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="FileConvention"/> class.
		/// </summary>
		public FileConvention()
		{
			MaxFailoverCheckPeriod = TimeSpan.FromMinutes(5);
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			AllowMultipuleAsyncOperations = true;
			IdentityPartsSeparator = "/";
		}

		private Dictionary<Type, PropertyInfo> idPropertyCache = new Dictionary<Type, PropertyInfo>();

		/// <summary>
		/// How should we behave in a replicated environment when we can't 
		/// reach the primary node and need to failover to secondary node(s).
		/// </summary>
		public FailoverBehavior FailoverBehavior { get; set; }

		/// <summary>
		/// Clone the current conventions to a new instance
		/// </summary>
		public FileConvention Clone()
		{
			return (FileConvention)MemberwiseClone();
		}

		public FailoverBehavior FailoverBehaviorWithoutFlags
		{
			get { return FailoverBehavior & (~FailoverBehavior.ReadFromAllServers); }
		}

		/// <summary>
		/// The maximum amount of time that we will wait before checking
		/// that a failed node is still up or not.
		/// Default: 5 minutes
		/// </summary>
		public TimeSpan MaxFailoverCheckPeriod { get; set; }

		/// <summary>
		/// Enable multipule async operations
		/// </summary>
		public bool AllowMultipuleAsyncOperations { get; set; }

		/// <summary>
		/// Gets or sets the function to find the identity property.
		/// </summary>
		/// <value>The find identity property.</value>
		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }

		/// <summary>
		/// Gets or sets the identity parts separator used by the HiLo generators
		/// </summary>
		/// <value>The identity parts separator.</value>
		public string IdentityPartsSeparator { get; set; }

		/// <summary>
		/// Gets the identity property.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public PropertyInfo GetIdentityProperty(Type type)
		{
			PropertyInfo info;
			var currentIdPropertyCache = idPropertyCache;
			if (currentIdPropertyCache.TryGetValue(type, out info))
				return info;

			var identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);

			if (identityProperty != null && identityProperty.DeclaringType != type)
			{
				var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
				identityProperty = propertyInfo ?? identityProperty;
			}

			idPropertyCache = new Dictionary<Type, PropertyInfo>(currentIdPropertyCache)
			{
				{type, identityProperty}
			};

			return identityProperty;
		}

		private static IEnumerable<PropertyInfo> GetPropertiesForType(Type type)
		{
			foreach (var propertyInfo in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
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
