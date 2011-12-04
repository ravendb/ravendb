// -----------------------------------------------------------------------
//  <copyright file="ShortViewOfJson.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public static class ShortViewOfJson
	{
		private static void WriteJsonObject(RavenJObject ravenJObject, StringWriter sw, int margin, int indent = 0)
		{
			sw.WriteLine('{');
			indent += 1;
			foreach (var item in ravenJObject)
			{
				Indent(sw, indent);
				sw.Write(item.Key);
				sw.Write(": ");
				WriteValue(item.Value, sw, margin, indent);
				sw.WriteLine();
			}
			indent -= 1;
			Indent(sw, indent);
			sw.Write('}');
		}

		private static void WriteValue(RavenJToken token, StringWriter sw, int margin, int indent)
		{
			switch (token.Type)
			{
				case JTokenType.Array:
					WriteJsonArray((RavenJArray)token, sw, margin, indent);
					break;
				case JTokenType.Object:
					WriteJsonObject((RavenJObject)token, sw, margin, indent);
					break;
				case JTokenType.Null:
					sw.Write("null");
					break;
				case JTokenType.String:
					sw.Write('"');
					sw.Write(token.ToString()
								.NormalizeWhitespace()
								.ShortViewOfString(margin - 2)
						);
					sw.Write('"');
					break;
				default:
					sw.Write(token.ToString().ShortViewOfString(margin));
					break;
			}
		}

		private static void WriteJsonArray(RavenJArray array, StringWriter sw, int margin, int indent = 0)
		{
			sw.WriteLine('[');
			indent += 1;
			var isFirstItem = true;
			foreach (var token in array.Values())
			{
				if (isFirstItem)
					isFirstItem = false;
				else
					sw.WriteLine(',');
				Indent(sw, indent);
				WriteValue(token, sw, margin, indent);
			}
			sw.WriteLine();
			indent -= 1;
			Indent(sw, indent);
			sw.Write(']');
		}

		private static void Indent(StringWriter sw, int indent)
		{
			if (indent > 0)
				sw.Write(new string(' ', indent * 2));
		}

		public static string GetContentDataWithMargin(RavenJObject dataAsJson, int margin)
		{
			margin = Math.Max(4, margin);
			var sw = new StringWriter();
			WriteJsonObject(dataAsJson, sw, margin);
			return sw.ToString();
		}
	}
}