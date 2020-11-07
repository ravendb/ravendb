//-----------------------------------------------------------------------
// <copyright file="JsonLuceneDateTimeConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    /// <summary>
    /// Convert a lucene data format to and from json values
    /// </summary>
    internal sealed class JsonLuceneDateTimeConverter : JsonConverter
    {
        // 17 numeric characters on a datetime field == Lucene datetime
        private static readonly Regex LuceneDateTimePattern = new Regex(@"\d{17}", RegexOptions.Compiled);

        public static readonly JsonLuceneDateTimeConverter Instance = new JsonLuceneDateTimeConverter();

        private JsonLuceneDateTimeConverter()
        {
        }

        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <param name="writer">The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.</param>
        /// <param name="value">The value.</param>
        /// <param name="serializer">The calling serializer.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        /// <param name="reader">The <see cref="T:Newtonsoft.Json.JsonReader"/> to read from.</param>
        /// <param name="objectType">Type of the object.</param>
        /// <param name="existingValue">The existing value of object being read.</param>
        /// <param name="serializer">The calling serializer.</param>
        /// <returns>The object value.</returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value is string input && LuceneDateTimePattern.IsMatch(input))
            {
                var stringToDate = DateTools.StringToDate(input);
                if (objectType == typeof(DateTimeOffset) || objectType == typeof(DateTimeOffset?))
                    return new DateTimeOffset(stringToDate, DateTimeOffset.Now.Offset);
                return DateTime.SpecifyKind(stringToDate, DateTimeKind.Local);
            }
            return reader.Value;
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
            return
                objectType == typeof(DateTime) ||
                objectType == typeof(DateTimeOffset) ||
                objectType == typeof(DateTime?) ||
                objectType == typeof(DateTimeOffset?);
        }

        /// <summary>
        /// Gets a value indicating whether this <see cref="T:Newtonsoft.Json.JsonConverter"/> can write JSON.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this <see cref="T:Newtonsoft.Json.JsonConverter"/> can write JSON; otherwise, <c>false</c>.
        /// </value>
        public override bool CanWrite => false;
    }
}
