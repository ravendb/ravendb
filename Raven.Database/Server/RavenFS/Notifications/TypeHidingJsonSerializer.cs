using System;
using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Raven.Client.RavenFS;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Notifications
{
	public class TypeHidingJsonSerializer
	{
		private static readonly JsonSerializerSettings Settings;

		static TypeHidingJsonSerializer()
		{
			Settings = new JsonSerializerSettings
			{
				Binder = new TypeHidingBinder(),
				TypeNameHandling = TypeNameHandling.Auto,
			};
		}

		public string Stringify(object obj)
		{
			return JsonConvert.SerializeObject(obj, Formatting.None, Settings);
		}

		public object Parse(string json)
		{
			return JsonConvert.DeserializeObject(json, Settings);
		}

		public object Parse(string json, Type targetType)
		{
			return JsonConvert.DeserializeObject(json, targetType, Settings);
		}

		public T Parse<T>(string json)
		{
			return JsonConvert.DeserializeObject<T>(json, Settings);
		}
	}

	/// <summary>
	///     We don't want to pollute our API with details about the types of our notification objects, so we bind
	///     based just on the type name, and assume the rest.
	/// </summary>
	internal class TypeHidingBinder : SerializationBinder
	{
		private readonly ConcurrentDictionary<string, Type> cachedTypes = new ConcurrentDictionary<string, Type>();

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
