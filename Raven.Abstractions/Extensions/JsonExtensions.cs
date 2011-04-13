//-----------------------------------------------------------------------
// <copyright file="JsonExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Json.Linq;

namespace Raven.Abstractions.Extensions
{
	/// <summary>
	/// Json extensions 
	/// </summary>
	public static class JsonExtensions
	{
	    public static RavenJObject ToJObject(object result)
        {
#if !NET_3_5
            var dynamicJsonObject = result as Linq.DynamicJsonObject;
            if (dynamicJsonObject != null)
                return dynamicJsonObject.Inner;
#endif
            if (result is string || result is ValueType)
				return new RavenJObject { { "Value", new RavenJValue(result) } };

            return RavenJObject.FromObject(result, CreateDefaultJsonSerializer());
        }

		/// <summary>
		/// Convert a byte array to a RavenJObject
		/// </summary>
		public static RavenJObject ToJObject(this byte [] self)
		{
			return RavenJObject.Load(new BsonReader(new MemoryStream(self))
			{
				DateTimeKindHandling = DateTimeKind.Utc,
			});
		}

        /// <summary>
        /// Convert a byte array to a RavenJObject
        /// </summary>
        public static RavenJObject ToJObject(this Stream self)
        {
            return RavenJObject.Load(new BsonReader(self)
            {
                DateTimeKindHandling = DateTimeKind.Utc,
            });
        }

		/// <summary>
		/// Convert a RavenJToken to a byte array
		/// </summary>
		public static byte[] ToBytes(this RavenJToken self)
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
        /// Convert a RavenJToken to a byte array
        /// </summary>
        public static void WriteTo(this RavenJToken self, Stream stream)
        {
            self.WriteTo(new BsonWriter(stream)
            {
                DateTimeKindHandling = DateTimeKind.Unspecified
            });
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
		public static T JsonDeserialization<T>(this RavenJObject self)
		{
			return (T)new JsonSerializer().Deserialize(new RavenJTokenReader(self), typeof(T));
		}

		public static JsonSerializer CreateDefaultJsonSerializer()
		{
			var jsonSerializer = new JsonSerializer();
			foreach (var defaultJsonConverter in Default.Converters)
			{
				jsonSerializer.Converters.Add(defaultJsonConverter);
			}
			return jsonSerializer;
		}

	}
}
