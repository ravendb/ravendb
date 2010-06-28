using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Json
{
	public static class JsonExtensions
	{
		public static JObject ToJObject(this byte [] self)
		{
			return JObject.Load(new BsonReader(new MemoryStream(self))
			{
				DateTimeKindHandling = DateTimeKind.Utc,
			});
		}

		public static byte[] ToBytes(this JToken self)
		{
			using (var memoryStream = new MemoryStream())
			{
				self.WriteTo(new BsonWriter(memoryStream)
				{
					NoDateTimeUniversalConversion = true
				});
				return memoryStream.ToArray();
			}
		}

		public static T JsonDeserialization<T>(this byte [] self)
		{
			return (T)new JsonSerializer().Deserialize(new BsonReader(new MemoryStream(self)), typeof(T));
		}

		public static T JsonDeserialization<T>(this JObject self)
		{
			return (T)new JsonSerializer().Deserialize(new JTokenReader(self), typeof(T));
		}
	}
}