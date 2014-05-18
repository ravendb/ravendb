using System;
using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.RavenFS
{
	public static class NotificationJSonUtilities
	{
		private static JsonSerializerSettings settings;

		static NotificationJSonUtilities()
		{
			settings = new JsonSerializerSettings
			{
				Binder = new TypeHidingBinder(),
				TypeNameHandling = TypeNameHandling.Auto
			};
		}

		public static T Parse<T>(string json)
		{
			return JsonConvert.DeserializeObject<T>(json, settings);
		}

		/// <summary>
		/// We don't want to pollute our API with details about the types of our notification objects, so we bind
		/// based just on the type name, and assume the rest.
		/// </summary>
		private class TypeHidingBinder : SerializationBinder
		{
			ConcurrentDictionary<string, Type> cachedTypes = new ConcurrentDictionary<string, Type>();

			public override Type BindToType(string assemblyName, string typeName)
			{
				Type type;

				if (!cachedTypes.TryGetValue(typeName, out type))
				{
					var @namespace = typeof(Notification).Namespace;
					var fullTypeName = @namespace + "." + typeName;
					type = Type.GetType(fullTypeName);

					cachedTypes.TryAdd(typeName, type);
				}

				return type;
			}

			public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
			{
				assemblyName = null;
				typeName = serializedType.Name;
			}
		}
	}
}
