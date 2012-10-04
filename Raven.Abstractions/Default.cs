using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions
{
	public static class Default
	{
		public static readonly string[] OnlyDateTimeFormat = new[]
		{
			"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"
		};

		/// <remarks>
		/// 'r' format is used on the in metadata, because it's delivered as http header. 
		/// </remarks>
		public static readonly string[] DateTimeFormatsToRead = new[]
		{
			"o", 
			"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff", 
			"yyyy-MM-ddTHH:mm:ss.fffffffzzz", 
			"yyyy-MM-ddTHH:mm:ss.FFFFFFFK", 
			"r",  
			"yyyy-MM-ddTHH:mm:ss.FFFK"
		};

		public static readonly string DateTimeOffsetFormatsToWrite = "o";
		public static readonly string DateTimeFormatsToWrite = "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff";

		public static readonly JsonConverter[] Converters = new JsonConverter[]
		{
			new JsonEnumConverter(),
			new JsonToJsonConverter(),
			new JsonDateTimeISO8601Converter(),
			new JsonDateTimeOffsetConverter(),
			new JsonDictionaryDateTimeKeysConverter(),
		};
	}
}
