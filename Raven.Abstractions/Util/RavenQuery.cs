//-----------------------------------------------------------------------
// <copyright file="RavenQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Text;

namespace Raven.Abstractions.Util
{
	/// <summary>
	/// Helper class that provide a way to escape query terms
	/// </summary>
	public static class RavenQuery
	{
		/// <summary>
		/// Escapes Lucene operators and quotes phrases
		/// </summary>
		public static string Escape(string term)
		{
			return Escape(term, false, true);
		}

		/// <summary>
		/// Escapes Lucene operators and quotes phrases
		/// </summary>
		/// <returns>escaped term</returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
		/// </remarks>
		public static string Escape(string term, bool allowWildcards, bool makePhrase)
		{
			// method doesn't allocate a StringBuilder unless the string requires escaping
			// also this copies chunks of the original string into the StringBuilder which
			// is far more efficient than copying character by character because StringBuilder
			// can access the underlying string data directly

			if (string.IsNullOrEmpty(term))
			{
				return "\"\"";
			}

			bool isPhrase = false;
			int start = 0;
			int length = term.Length;
			StringBuilder buffer = null;

			for (int i = start; i < length; i++)
			{
				char ch = term[i];
				switch (ch)
				{
					// should wildcards be included or excluded here?
					case '*':
					case '?':
						{
							if (allowWildcards)
							{
								break;
							}
							goto case '\\';
						}
					case '+':
					case '-':
					case '&':
					case '|':
					case '!':
					case '(':
					case ')':
					case '{':
					case '}':
					case '[':
					case ']':
					case '^':
					case '"':
					case '~':
					case ':':
					case '\\':
						{
							if (buffer == null)
							{
								// allocate builder with headroom
								buffer = new StringBuilder(length * 2);
							}

							if (i > start)
							{
								// append any leading substring
								buffer.Append(term, start, i - start);
							}

							buffer.Append('\\').Append(ch);
							start = i + 1;
							break;
						}
					case ' ':
					case '\t':
						{
							if (!isPhrase && makePhrase)
							{
								if (buffer == null)
								{
									// allocate builder with headroom
									buffer = new StringBuilder(length * 2);
								}

								buffer.Insert(0, "\"");
								isPhrase = true;
							}
							break;
						}
				}
			}

			if (buffer == null)
			{
				// no changes required
				switch (term)
				{
					case "OR":
						return "\"OR\"";
					case "AND":
						return "\"AND\"";
					default:
						return term;
				}
			}

			if (length > start)
			{
				// append any trailing substring
				buffer.Append(term, start, length - start);
			}

			if (isPhrase)
			{
				// quoted phrase
				buffer.Append('"');
			}

			return buffer.ToString();
		}
	}
}
