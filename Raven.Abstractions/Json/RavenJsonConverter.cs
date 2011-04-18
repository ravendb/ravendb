using System;
using System.Linq;
using Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public abstract class RavenJsonConverter : JsonConverter
	{
		protected object DeferReadToNextConverter(JsonReader reader, Type objectType, JsonSerializer serializer, object existingValue)
		{
			var anotherConverter =
				serializer.Converters.Skip(serializer.Converters.IndexOf(this) + 1)
					.Where(x => x.CanConvert(objectType))
					.FirstOrDefault();
			if (anotherConverter != null)
				return anotherConverter.ReadJson(reader, objectType, existingValue, serializer);
			return reader.Value;
		}

	}
}