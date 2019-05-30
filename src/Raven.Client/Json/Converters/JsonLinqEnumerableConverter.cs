using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json;

namespace Raven.Client.Json.Converters
{
    /// <summary>
    /// This converter is used when a property is a Linq-To-Entities query, enumerating and 
    /// then serializing it as a json array.
    /// </summary>
    public sealed class JsonLinqEnumerableConverter : JsonConverter
    {
        public static readonly JsonLinqEnumerableConverter Instance = new JsonLinqEnumerableConverter();
        private static readonly ConcurrentDictionary<Type, bool> _converterCache = new ConcurrentDictionary<Type, bool>();

        private JsonLinqEnumerableConverter()
        {
        }

        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <remarks>This will enumerate the Linq-To-Entities query before serializing it to json array</remarks>
        /// <param name="writer">The <see cref="JsonWriter"/> to write to.</param>
        /// <param name="value">The value.</param>
        /// <param name="serializer">The calling serializer.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var enumerable = (IEnumerable)value;

            writer.WriteStartArray();
            foreach (var item in enumerable)
                serializer.Serialize(writer, item);

            writer.WriteEndArray();
        }

        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        /// <remarks>
        /// this converter will never be needed to deserialize from json -
        ///  built-in converter is enough as Json.Net serializes any collection - including IEnumerable{T} to json arrays.
        /// </remarks>
        /// <param name="reader">The <see cref="JsonReader"/> to read from.</param>
        /// <param name="objectType">Type of the object.</param>
        /// <param name="existingValue">The existing value of object being read.</param>
        /// <param name="serializer">The calling serializer.</param>
        /// <returns>executing this method will throw <see cref="NotSupportedException">NotSupportedException</see> since this converter should not be used for reading</returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            //this converter will never be needed to deserialize from json - built-in converter is enough as
            //Json.Net serializes any collection - including IEnumerable<T> to json arrays.
            throw new NotSupportedException($@"{nameof(JsonLinqEnumerableConverter)} should not be used to deserialize collections from json - if this exception gets thrown, it is probably a bug.");
        }

        /// <summary>
        /// Determines whether this instance can convert the specified object type.
        /// </summary>
        /// <param name="objectType">Type of the object.</param>
        /// <returns>
        /// 	<c>true</c> if this instance can convert the specified object type; otherwise, <c>false</c>.
        /// </returns>
        public override bool CanConvert(Type objectType)
        {
            if (_converterCache.TryGetValue(objectType, out var canConvert))
                return canConvert;

            if (objectType.Namespace == null)
            {
                canConvert = false;
            }
            else if (objectType == typeof(string))
            {
                canConvert = false;
            }
            else if (objectType.GetTypeInfo().IsClass == false)
            {
                canConvert = false;
            }
            else
            {
                canConvert = false;
                foreach (var interfaceType in objectType.GetInterfaces())
                {
                    if (interfaceType.GetTypeInfo().IsGenericType == false)
                        continue;

                    var genericInterfaceType = interfaceType.GetGenericTypeDefinition();
                    if (typeof(IEnumerable<>) == genericInterfaceType && objectType.Namespace.StartsWith("System.Linq"))
                    {
                        canConvert = true;
                        break;
                    }
                }
            }

            _converterCache.TryAdd(objectType, canConvert);
            return canConvert;
        }
    }
}
