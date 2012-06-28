//-----------------------------------------------------------------------
// <copyright file="QueryBuilder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using System.Text.RegularExpressions;

using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	public static class QueryBuilder
	{
		static readonly Regex untokenizedQuery = new Regex(@"([\w\d_]+?):\s*(\[\[.+?\]\])", RegexOptions.Compiled);
		static readonly Regex searchQuery = new Regex(@"([\w\d_]+?):\s*(\<\<.+?\>\>)(^[\d.]+)?", RegexOptions.Compiled);
		static readonly Regex dateQuery = new Regex(@"([\w\d_]+?):\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7})", RegexOptions.Compiled);

		public static Query BuildQuery(string query, PerFieldAnalyzerWrapper analyzer)
		{
			return BuildQuery(query, null, analyzer);
		}

		public static Query BuildQuery(string query, string defaultField, PerFieldAnalyzerWrapper analyzer)
		{
			var originalQuery = query;
			Analyzer keywordAnalyzer = new KeywordAnalyzer();
			try
			{
				var queryParser = new RangeQueryParser(Version.LUCENE_29, defaultField ?? string.Empty, analyzer);
				query = PreProcessUntokenizedTerms(query, queryParser);
				query = PreProcessSearchTerms(query);
				query = PreProcessDateTerms(query, queryParser);
				queryParser.SetAllowLeadingWildcard(true); // not the recommended approach, should rather use ReverseFilter
				return queryParser.Parse(query);
			}
			catch (ParseException pe)
			{
				if (originalQuery == query)
					throw new ParseException("Could not parse: '" + query +"'", pe);
				throw new ParseException("Could not parse modified query: '" + query + "' original was: '" + originalQuery +"'", pe);

			}
			finally
			{
				keywordAnalyzer.Close();
			}
		}

		private static string PreProcessDateTerms(string query, RangeQueryParser queryParser)
		{
			var searchMatches = dateQuery.Matches(query);
			if (searchMatches.Count < 1)
				return query;

			var queryStringBuilder = new StringBuilder(query);
			for (var i = searchMatches.Count - 1; i >= 0; i--) // reversing the scan so we won't affect positions of later items
			{
				var searchMatch = searchMatches[i];
				var field = searchMatch.Groups[1].Value;
				var termReplacement = searchMatch.Groups[2].Value;

				var replaceToken = queryParser.ReplaceToken(field, termReplacement);
				queryStringBuilder.Remove(searchMatch.Index, searchMatch.Length);
				queryStringBuilder
					.Insert(searchMatch.Index, field)
					.Insert(searchMatch.Index + field.Length, ":")
					.Insert(searchMatch.Index + field.Length + 1, replaceToken);
			}

			return queryStringBuilder.ToString();
		}

		private static string PreProcessSearchTerms(string query)
		{
			var searchMatches = searchQuery.Matches(query);
			if (searchMatches.Count < 1)
				return query;

			var queryStringBuilder = new StringBuilder(query);
			for (var i = searchMatches.Count - 1; i >= 0; i--) // reversing the scan so we won't affect positions of later items
			{
				var searchMatch = searchMatches[i];
				var field = searchMatch.Groups[1].Value;
				var terms = searchMatch.Groups[2].Value.Substring(2, searchMatch.Groups[2].Length - 4);

				string boost = "";
				if (string.IsNullOrWhiteSpace(searchMatch.Groups[3].Value) == false)
				{
					boost = searchMatch.Groups[3].Value;
				}

				queryStringBuilder.Remove(searchMatch.Index, searchMatch.Length);
				queryStringBuilder.Insert(searchMatch.Index, '(');
				var len = searchMatch.Index;
				foreach (var term in terms.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
				{
					switch (term) // ignore invalid options
					{
						case "OR":
						case "AND":
						case "||":
						case "&&":
							continue;
					}

					var termQuery = new StringBuilder().Append(field).Append(':').Append(term).Append(boost).Append(' ');
					len += termQuery.Length;
					queryStringBuilder.Insert(searchMatch.Index + 1, termQuery);
				}
				queryStringBuilder.Insert(len, ')');
			}
			return queryStringBuilder.ToString();
		}

		/// <summary>
		/// Detects untokenized fields and sets as NotAnalyzed in analyzer
		/// </summary>
		private static string PreProcessUntokenizedTerms(string query, RangeQueryParser queryParser)
		{
			var untokenizedMatches = untokenizedQuery.Matches(query);
			if (untokenizedMatches.Count < 1)
				return query;

			var sb = new StringBuilder(query);

			// process in reverse order to leverage match string indexes
			for (var i = untokenizedMatches.Count; i > 0; i--)
			{
				var match = untokenizedMatches[i - 1];

				// specify that term for this field should not be tokenized
				var value = match.Groups[2].Value;

				var rawTerm = value.Substring(2, value.Length - 4);
				queryParser.SetUntokenized(match.Groups[1].Value, Unescape(rawTerm));

				var term = match.Groups[2];

				// introduce " " around the term
				var startIndex = term.Index;
				var length = term.Length - 2;
				if (sb[startIndex + length - 1] != '"')
				{
					sb.Insert(startIndex + length, '"');
					length += 1;
				}
				if (sb[startIndex + 2] != '"')
				{
					sb.Insert(startIndex + 2, '"');
					length += 1;
				}
				// remove enclosing "[[" "]]" from term value (again in reverse order)
				sb.Remove(startIndex + length, 2);
				sb.Remove(startIndex, 2);
			}

			return sb.ToString();
		}

		public static string Unescape(string term)
		{
			// method doesn't allocate a StringBuilder unless the string requires unescaping
			// also this copies chunks of the original string into the StringBuilder which
			// is far more efficient than copying character by character because StringBuilder
			// can access the underlying string data directly

			if (string.IsNullOrEmpty(term))
			{
				return term;
			}

			bool isPhrase = term.StartsWith("\"") && term.EndsWith("\"");
			int start = 0;
			int length = term.Length;
			StringBuilder buffer = null;
			char prev = '\0';
			for (int i = start; i < length; i++)
			{
				char ch = term[i];
				if (prev != '\\')
				{
					prev = ch;
					continue;
				}
				prev = ch;
				switch (ch)
				{
					case '*':
					case '?':
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
							// append any leading substring
							buffer.Append(term, start, i - start - 1);

							buffer.Append(ch);
							start = i + 1;
							break;
						}
				}
			}

			if (buffer == null)
			{
				if (isPhrase)
					return term.Substring(1, term.Length - 2);
				// no changes required
				return term;
			}

			if (length > start)
			{
				// append any trailing substring
				buffer.Append(term, start, length - start);
			}

			return buffer.ToString();
		}
	}
}
