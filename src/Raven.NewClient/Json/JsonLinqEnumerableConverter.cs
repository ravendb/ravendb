using System;
using System.Collections;
using System.Collections.Generic;
using  Raven.Imports.Newtonsoft.Json;
using  Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Abstractions.Json
{
    /// <summary>
    /// This converter is used when a property is a Linq-To-Entities query, enumerating and 
    /// then serializing it as a json array.
    /// </summary>
    public class JsonLinqEnumerableConverter : JsonConverter
    {
        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <remarks>This will enumerate the Linq-To-Entities query before serializing it to json array</remarks>
        /// <param name="writer">The <see cref="JsonWriter"/> to write to.</param>
        /// <param name="value">The value.</param>
        /// <param name="serializer">The calling serializer.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var enumerable = (IEnumerable) value;
            writer.WriteStartArray();
            foreach(var item in enumerable)
                writer.WriteValue(item);
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
            throw new NotSupportedException(@"JsonLinqEnumerableConverter should not be used to deserialize collections from json - 
                                            if this exception gets thrown, it is probably a bug.");
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
            if (objectType.Namespace == null)
                return false;
            return ReflectionUtils.ImplementsGenericDefinition(objectType,typeof(IEnumerable<>)) &&
                    objectType.Namespace.StartsWith("System.Linq");
        }
    }
}
