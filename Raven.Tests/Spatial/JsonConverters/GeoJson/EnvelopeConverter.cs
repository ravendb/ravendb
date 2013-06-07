using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using GeoAPI.Geometries;

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
	public class EnvelopeConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            Envelope envelope = value as Envelope;
            if (envelope == null) return;

            writer.WritePropertyName("bbox");
            writer.WriteStartArray();
            writer.WriteValue(envelope.MinX);
            writer.WriteValue(envelope.MinY);
            writer.WriteValue(envelope.MaxX);
            writer.WriteValue(envelope.MaxY);
            writer.WriteEndArray();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            Debug.Assert(reader.TokenType == JsonToken.PropertyName);
            Debug.Assert((string)reader.Value == "bbox");

            JArray envelope = serializer.Deserialize<JArray>(reader);
            Debug.Assert(envelope.Count == 4);

            double minX = Double.Parse((string)envelope[0]);
            double minY = Double.Parse((string)envelope[1]);
            double maxX = Double.Parse((string)envelope[2]);
            double maxY = Double.Parse((string)envelope[3]);

            Debug.Assert(minX <= maxX);
            Debug.Assert(minY <= maxY);

            return new Envelope(minX, minY, maxX, maxY);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Envelope);
        }
    }
}