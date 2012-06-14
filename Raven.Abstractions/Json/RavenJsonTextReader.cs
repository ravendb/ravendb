using System;
using System.Globalization;
using System.IO;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public class RavenJsonTextReader : JsonTextReader
	{
		public RavenJsonTextReader(TextReader reader)
			: base(reader)
		{
			DateParseHandling = DateParseHandling.None;
		}

		private static TimeSpan ReadOffset(string offsetText)
		{
			bool negative = (offsetText[0] == '-');

			int hours = int.Parse(offsetText.Substring(1, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);
			int minutes = 0;
			if (offsetText.Length >= 5)
				minutes = int.Parse(offsetText.Substring(3, 2), NumberStyles.Integer, CultureInfo.InvariantCulture);

			TimeSpan offset = TimeSpan.FromHours(hours) + TimeSpan.FromMinutes(minutes);
			if (negative)
				offset = offset.Negate();

			return offset;
		}

		public static DateTime ParseDateMicrosoft(string text)
		{
			string value = text.Substring(6, text.Length - 8);

			int index = value.IndexOf('+', 1);

			if (index == -1)
				index = value.IndexOf('-', 1);

			if (index != -1)
			{
				value = value.Substring(0, index);
			}

			long javaScriptTicks = long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

			DateTime utcDateTime = JsonConvert.ConvertJavaScriptTicksToDateTime(javaScriptTicks);
			return utcDateTime;
		}
	}
}