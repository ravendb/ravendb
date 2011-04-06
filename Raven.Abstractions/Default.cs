using Newtonsoft.Json;
using Raven.Abstractions.Json;
using Raven.Database.Json;
using Raven.Http.Json;

namespace Raven.Abstractions
{
	public static class Default
	{
        public static string[] DateTimeFormatsToRead = new[] { "o", "yyyy-MM-ddTHH:mm:ss.fffffffzzz" };
        public static string DateTimeFormatsToWrite = "o" ;

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