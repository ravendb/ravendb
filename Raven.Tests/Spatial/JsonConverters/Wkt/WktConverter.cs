using System;
using GeoAPI.Geometries;
using NetTopologySuite.IO;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Tests.Spatial.JsonConverters.Wkt
{
	public class WktConverter : JsonConverter
	{
		private readonly WKTReader wktReader = new WKTReader();
		private readonly WKTWriter wktWriter = new WKTWriter();

		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			writer.WriteValue(wktWriter.Write((IGeometry)value));
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			if (reader.TokenType == JsonToken.Null)
				return null;

			return wktReader.Read((string)reader.Value);
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof(IGeometry).IsAssignableFrom(objectType);
		}
	}
}