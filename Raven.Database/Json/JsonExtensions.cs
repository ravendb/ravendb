using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
	/// <summary>
	/// Json extensions 
	/// </summary>
	public static class JsonExtensions
	{
		/// <summary>
		/// Convert a byte array to a JObject
		/// </summary>
		public static JObject ToJObject(this byte [] self)
		{
			return JObject.Load(new BsonReader(new MemoryStream(self))
			{
				DateTimeKindHandling = DateTimeKind.Utc,
			});
		}

		/// <summary>
		/// Convert a JToken to a byte array
		/// </summary>
		public static byte[] ToBytes(this JToken self)
		{
			using (var memoryStream = new MemoryStream())
			{
				self.WriteTo(new BsonWriter(memoryStream)
				{
					DateTimeKindHandling = DateTimeKind.Unspecified
				});
				return memoryStream.ToArray();
			}
		}


		/// <summary>
		/// Deserialize a <param name="self"/> to an instance of <typeparam name="T"/>
		/// </summary>
		public static T JsonDeserialization<T>(this byte [] self)
		{
			return (T)new JsonSerializer().Deserialize(new BsonReader(new MemoryStream(self)), typeof(T));
		}

		/// <summary>
		/// Deserialize a <param name="self"/> to an instance of<typeparam name="T"/>
		/// </summary>
		public static T JsonDeserialization<T>(this JObject self)
		{
			return (T)new JsonSerializer().Deserialize(new JTokenReader(self), typeof(T));
		}
	}
}