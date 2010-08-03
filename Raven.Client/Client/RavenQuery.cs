using System.Text;

namespace Raven.Client.Client
{
	public static class RavenQuery
	{
		/// <summary>
		/// Escapes Lucene operators and quotes phrases
		/// </summary>
		/// <param name="term"></param>
		/// <param name="allowWildcards"></param>
		/// <returns>escaped term</returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
		/// </remarks>
		public static string Escape(string term, bool allowWildcards)
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
							if (!isPhrase)
							{
								if (buffer == null)
								{
									// allocate builder with headroom
									buffer = new StringBuilder(length * 2);
								}

								buffer.Insert(0, '"');
								isPhrase = true;
							}
							break;
						}
				}
			}

			if (buffer == null)
			{
				// no changes required
				return term;
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