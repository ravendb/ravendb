using Raven.Imports.Newtonsoft.Json;
using System;
using NetTopologySuite.CoordinateSystems;

// From: https://code.google.com/p/nettopologysuite/source/browse/#svn%2Ftrunk%2FNetTopologySuite.IO%2FNetTopologySuite.IO.GeoJSON
namespace Raven.Tests.Spatial.JsonConverters.GeoJson
{
	/// <summary>
    /// Converts ICRSObject object to its JSON representation.
    /// </summary>
    public class ICRSObjectConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (writer == null)
                throw new ArgumentNullException("writer");
            if (serializer == null)
                throw new ArgumentNullException("serializer");

            ICRSObject crs = value as ICRSObject;
            if (crs == null)
                return;
                
            writer.WriteStartObject();
            writer.WritePropertyName("type");
            string type = Enum.GetName(typeof(CRSTypes), crs.Type);        
            writer.WriteValue(type.ToLowerInvariant());
            CRSBase crsb = value as CRSBase;
            if (crsb != null)
            {
                writer.WritePropertyName("properties");
                serializer.Serialize(writer, crsb.Properties);
            }
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(ICRSObject).IsAssignableFrom(objectType);
        }
    }
}