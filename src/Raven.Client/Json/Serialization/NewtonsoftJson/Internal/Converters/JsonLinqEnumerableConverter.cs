using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    /// <summary>
    /// This converter is used when a property is a Linq-To-Entities query, enumerating and
    /// then serializing it as a json array.
    /// </summary>
    internal sealed class JsonLinqEnumerableConverter : JsonConverter
    {
        public static readonly JsonLinqEnumerableConverter Instance = new JsonLinqEnumerableConverter();

        private Dictionary<Type, bool> _canConvertCache = new ();

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
        /// Gets a value indicating whether this <see cref="JsonConverter"/> can read JSON.
        /// </summary>
        /// <value><c>true</c> if this <see cref="JsonConverter"/> can read JSON; otherwise, <c>false</c>.</value>
        public override bool CanRead => false;

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
            throw new NotSupportedException($"{nameof(JsonLinqEnumerableConverter)} should not be used to deserialize collections from json - if this exception gets thrown, it is probably a bug.");
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
            if (objectType.Namespace == null || objectType == typeof(string) || objectType.IsClass == false)
                return false;

            if (!_canConvertCache.TryGetValue(objectType, out bool canConvert))
            {
                canConvert = false;
                foreach (var interfaceType in objectType.GetInterfaces())
                {
                    if (interfaceType.IsGenericType == false)
                        continue;

                    var genericInterfaceType = interfaceType.GetGenericTypeDefinition();
                    if (typeof(IEnumerable<>) == genericInterfaceType && objectType.Namespace.StartsWith("System.Linq"))
                    {
                        canConvert = true;
                        break;
                    }
                }

                // PERF: We are expecting a race condition here, this is an optimistic switch-on-change scheme.
                //       It mostly works because the frequency of this call is very low. 
                _canConvertCache = new Dictionary<Type, bool>(_canConvertCache) { [objectType] = canConvert };
            }

            return canConvert;
        }
    }
}
