using Newtonsoft.Json;
using Raven.Database.Json;
using Raven.Http.Json;

namespace Raven.Abstractions.Json
{
	public static class Default
	{
		public static JsonConverter[] Converters = new JsonConverter[]
		{
			new JsonEnumConverter(),
#if !NET_3_5
			new JsonToJsonConverter(),
#endif
			new JsonDateTimeISO8601Converter(),
			new JsonDateTimeOffsetConverter()
		};
	}
}