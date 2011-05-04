using Newtonsoft.Json;
using Raven.Abstractions.Json;

namespace Raven.Abstractions
{
	public static class Default
	{
        public static string[] DateTimeFormatsToRead = new[] { "o", "yyyy-MM-ddTHH:mm:ss.fffffffzzz" };
        public static string DateTimeFormatsToWrite = "o" ;

		public static JsonConverter[] Converters = new JsonConverter[]
		{
			new JsonEnumConverter(),
			new JsonToJsonConverter(),
			new JsonDateTimeISO8601Converter(),
			new JsonDateTimeOffsetConverter()
		};
	}
}