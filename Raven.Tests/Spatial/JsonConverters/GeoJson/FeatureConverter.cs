using Raven.Imports.Newtonsoft.Json;
using System;
using NetTopologySuite.Features;

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
	/// <summary>
    /// Converts Feature object to its JSON representation.
    /// </summary>
    public class FeatureConverter : JsonConverter
    {
        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <param name="writer">The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.</param>
        /// <param name="value">The value.</param>
        /// <param name="serializer">The calling serializer.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (writer == null) 
                throw new ArgumentNullException("writer");
            if (serializer == null) 
                throw new ArgumentNullException("serializer");

            Feature feature = value as Feature;
            if (feature == null)
                return;

            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue("Feature");
            writer.WritePropertyName("geometry");
            serializer.Serialize(writer, feature.Geometry);
            serializer.Serialize(writer, feature.Attributes);
            writer.WriteEndObject();
        }

        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        /// <param name="reader">The <see cref="T:Newtonsoft.Json.JsonReader"/> to read from.</param>
        /// <param name="objectType">Type of the object.</param>
        /// <param name="existingValue">The existing value of object being read.</param>
        /// <param name="serializer">The calling serializer.</param>
        /// <returns>
        /// The object value.
        /// </returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Determines whether this instance can convert the specified object type.
        /// </summary>
        /// <param name="objectType">Type of the object.</param>
        /// <returns>
        ///   <c>true</c> if this instance can convert the specified object type; otherwise, <c>false</c>.
        /// </returns>
        public override bool CanConvert(Type objectType)
        {
            return typeof(Feature).IsAssignableFrom(objectType);
        }
    }
}