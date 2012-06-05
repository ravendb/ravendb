using System;
using Newtonsoft.Json;
using Raven.Abstractions.Json;

namespace Raven.Abstractions
{
	public static class Default
	{
		public static string[] OnlyDateTimeFormat = new[] { "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"};
		public static string[] DateTimeFormatsToRead = new[] { "o", "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff", "yyyy-MM-ddTHH:mm:ss.fffffffzzz", "yyyy-MM-ddTHH:mm:ss.fffzzz" };
		public static string  DateTimeOffsetFormatsToWrite = "o";
		public static string DateTimeFormatsToWrite= "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff";

		public static JsonConverter[] Converters = new JsonConverter[]
		{
			new JsonEnumConverter(),
			new JsonToJsonConverter(),
			new JsonDateTimeISO8601Converter(),
			new JsonDateTimeOffsetConverter(),
			new JsonDictionaryDateTimeKeysConverter(typeof(DateTime), typeof(DateTime?), typeof(DateTimeOffset), typeof(DateTimeOffset?)),
		};
	}
}