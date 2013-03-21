using Raven.Imports.Newtonsoft.Json;
using System;
using NetTopologySuite.Features;

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
	/// <summary>
    /// Converts FeatureCollection object to its JSON representation.
    /// </summary>
    public class FeatureCollectionConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (writer == null)
                throw new ArgumentNullException("writer");
            if (serializer == null)
                throw new ArgumentNullException("serializer");

            FeatureCollection coll = value as FeatureCollection;
            if (coll == null)
                return;

            writer.WriteStartObject();
            writer.WritePropertyName("features");
            serializer.Serialize(writer, coll.Features);
            writer.WritePropertyName("type");
            writer.WriteValue(coll.Type);
            if (coll.CRS != null)
            {
                writer.WritePropertyName("crs");
                serializer.Serialize(writer, coll.CRS);
            }            
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(FeatureCollection).IsAssignableFrom(objectType);
        }
    }
}