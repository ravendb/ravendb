// -----------------------------------------------------------------------
//  <copyright file="ShortViewOfJson.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Documents
{
	public static class ShortViewOfJson
	{
		private static void WriteJsonObject(RavenJObject ravenJObject, CountingWriter sw, int width, int numberOfLines)
		{
			sw.WriteLine("{");
			sw.PushIndent();

			foreach (var item in ravenJObject)
			{
                if (sw.LinesWritten > numberOfLines)
                {
                    break;
                }
				sw.Write(item.Key);
				sw.Write(": ");
                WriteValue(item.Value, sw, width, numberOfLines);
				sw.WriteLine("");
			}

			sw.PopIndent();
			sw.Write("}");
		}

        private static void WriteValue(RavenJToken token, CountingWriter sw, int width, int numberOfLines)
		{
			switch (token.Type)
			{
				case JTokenType.Array:
                    WriteJsonArray((RavenJArray)token, sw, width, numberOfLines);
					break;
				case JTokenType.Object:
                    WriteJsonObject((RavenJObject)token, sw, width, numberOfLines);
					break;
				case JTokenType.Null:
					sw.Write("null");
					break;
				case JTokenType.String:
					sw.Write("\"");
					sw.Write(token.ToString()
								.NormalizeWhitespace()
                                .TrimmedViewOfString(width - sw.CharactersOnCurrentLine -1)
						);
                    sw.Write("\"");
					break;
				default:
                    sw.Write(token.ToString().TrimmedViewOfString(width - sw.CharactersOnCurrentLine - 1));
					break;
			}
		}

        private static void WriteJsonArray(RavenJArray array, CountingWriter sw, int width, int numberOfLines)
		{
			sw.WriteLine("[");
			sw.PushIndent();

			var isFirstItem = true;
			foreach (var token in array.Values())
			{
                if (sw.LinesWritten >= numberOfLines)
                {
                    break;
                }

				if (isFirstItem)
					isFirstItem = false;
				else
					sw.WriteLine(",");
                WriteValue(token, sw, width, numberOfLines);
			}
			sw.WriteLine("");
			sw.PopIndent();
			sw.Write("]");
		}

	    public static string GetContentTrimmedToDimensions(RavenJObject dataAsJson, int widthInCharacters, int heightInLines)
	    {
	        var sw = new CountingWriter(2);

            WriteJsonObject(dataAsJson, sw, widthInCharacters, heightInLines);

	        return sw.ToString();
	    }

        private class CountingWriter
        {
            private readonly int indentSize;
            private int linesWritten;
            private int charctersOnCurrentLine;
            private StringWriter sw;
            private int indent;
            private bool needsIndent;

            public CountingWriter(int indentSize)
            {
                this.indentSize = indentSize;
                sw = new StringWriter();
            }

            public int LinesWritten
            {
                get { return linesWritten; }
            }

            public int CharactersOnCurrentLine
            {
                get { return charctersOnCurrentLine; }
            }

            public void Write(string value)
            {
                WriteIndentIfNeeded();

                charctersOnCurrentLine += value.Length;
                sw.Write(value);
            }

            private void WriteIndentIfNeeded()
            {
                if (needsIndent)
                {
                    sw.Write(new string(' ', indentSize * indent));
                    needsIndent = false;
                }
            }

            public void WriteLine(string value)
            {
                WriteIndentIfNeeded();

                sw.WriteLine(value);
                linesWritten++;
                charctersOnCurrentLine = 0;
                needsIndent = true;
            }

            public void PushIndent()
            {
                indent++;
            }

            public void PopIndent()
            {
                indent = Math.Max(indent-1,0);
            }

            public override string ToString()
            {
                return sw.ToString();
            }
        }
	}
}