//-----------------------------------------------------------------------
// <copyright file="QueryBuilder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using System.Text.RegularExpressions;

using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{	
	public static class QueryBuilder
	{
		static readonly Regex untokenizedQuery = new Regex(@"([\w\d_]+?):(\[\[.+?\]\])", RegexOptions.Compiled);

		public static Query BuildQuery(string query, PerFieldAnalyzerWrapper analyzer)
		{
			Analyzer keywordAnalyzer = null;
			try
			{
				query = PreProcessUntokenizedTerms(analyzer, query, ref keywordAnalyzer);
				var queryParser = new RangeQueryParser(Version.LUCENE_29, string.Empty, analyzer);
				queryParser.SetAllowLeadingWildcard(true); // not the recommended approach, should rather use ReverseFilter
				return queryParser.Parse(query);
			}
			finally
			{
				if (keywordAnalyzer != null)
					keywordAnalyzer.Close();
			}
		}

		/// <summary>
		/// Detects untokenized fields and sets as NotAnalyzed in analyzer
		/// </summary>
		private static string PreProcessUntokenizedTerms(PerFieldAnalyzerWrapper analyzer, string query, ref Analyzer keywordAnalyzer)
		{
			var untokenizedMatches = untokenizedQuery.Matches(query);
			if (untokenizedMatches.Count < 1)
				return query;

			var sb = new StringBuilder(query);

			// Initialize a KeywordAnalyzer
			// KeywordAnalyzer will not tokenize the values
			keywordAnalyzer = new KeywordAnalyzer();

			// process in reverse order to leverage match string indexes
			for (var i = untokenizedMatches.Count; i > 0; i--)
			{
				var match = untokenizedMatches[i - 1];

				// specify that term for this field should not be tokenized
				analyzer.AddAnalyzer(match.Groups[1].Value, keywordAnalyzer);

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
	}
}
