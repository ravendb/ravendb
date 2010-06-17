using System;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
	public static class QueryBuilder
	{
		static readonly Regex untokenizedQuery = new Regex(@"([+-]?)([\w\d_]+?):\[\[(.+?)\]\]", RegexOptions.Compiled);
		static readonly Regex rangeQuery = new Regex(@"([+-]?)([\w\d_]+_Range?):(({|\[)[ \w\d.]+?(}|\]))", RegexOptions.Compiled);
		static readonly Regex rangeValue = new Regex(@"({|\[) \s* (([\w\d]x[\w\d.]+)|(NULL)) \s* TO  \s* (([\w\d]x[\w\d.]+)|(NULL)) \s* (}|\])", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		static readonly Regex hangingConditionAtStart = new Regex(@"^ \s* (AND|OR)", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);
		static readonly Regex hangingConditionAtEnd = new Regex(@"(AND|OR) \s* $", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		public static Query BuildQuery(string query)
		{
			var untokenizedMatches = untokenizedQuery.Matches(query);
			var rangeMatches = rangeQuery.Matches(query);
		    var standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
		    try
		    {
                if (untokenizedMatches.Count == 0 && rangeMatches.Count == 0)
                    return new QueryParser(Version.LUCENE_29, "", standardAnalyzer).Parse(query);
                var sb = new StringBuilder(query);
                var booleanQuery = new BooleanQuery();
                AddUntokenizedTerms(untokenizedMatches, booleanQuery, sb, query);
		    	AddRangeTerms(rangeMatches, booleanQuery, sb, query);
                var remaining = sb.ToString();
		    	remaining = hangingConditionAtStart.Replace(remaining, "");
		    	remaining = hangingConditionAtEnd.Replace(remaining, "");
				remaining = remaining.Trim();
		    	if (remaining.Length > 0)
                {
                    booleanQuery.Add(new QueryParser(Version.LUCENE_29, "", standardAnalyzer).Parse(remaining), BooleanClause.Occur.SHOULD);
                }
                return booleanQuery;
		    }
		    finally
		    {
		        standardAnalyzer.Close();
		    }
		}

		private static void AddRangeTerms(MatchCollection rangeMatches, BooleanQuery booleanQuery, StringBuilder sb, string query)
		{
			foreach (Match match in rangeMatches)
			{
				var fieldName = match.Groups[2].Value;
				var rangeValueMatch = rangeValue.Match(match.Groups[3].Value);
				if(rangeValueMatch.Success == false) // this is a date, no change is required
					continue;

				var inclusiveRange = rangeValueMatch.Groups[1].Value == "[";

				var from = NumberUtil.StringToNumber(rangeValueMatch.Groups[2].Value);
				var to = NumberUtil.StringToNumber(rangeValueMatch.Groups[5].Value);

				NumericRangeQuery range = null;

				if (from is int || to is int)
					range = NumericRangeQuery.NewIntRange(fieldName, (int) (from ?? int.MinValue), (int) (to ?? int.MaxValue), inclusiveRange, inclusiveRange);

				if (from is long || to is long)
					range = NumericRangeQuery.NewLongRange(fieldName, (long)(from ?? long.MinValue), (long)(to ?? long.MaxValue), inclusiveRange, inclusiveRange);

				if (from is double || to is double)
					range = NumericRangeQuery.NewDoubleRange(fieldName, (double) (from ?? double.MinValue), (double) (to ?? double.MaxValue),
					                                         inclusiveRange, inclusiveRange);
			
				if (from is float || to is float)
					range = NumericRangeQuery.NewFloatRange(fieldName, (float)(from ?? float.MinValue), (float)(to ?? float.MaxValue), inclusiveRange, inclusiveRange);
				
				if(range == null)
					throw new InvalidOperationException("Could not understand how to parse: " + match.Value);

				booleanQuery.Add(range, GetOccur(query, match));

				sb.Replace(match.Value, "");
			}
		}

		private static void AddUntokenizedTerms(MatchCollection untokenizedMatches, BooleanQuery booleanQuery, StringBuilder sb, string query)
		{
			foreach (Match match in untokenizedMatches)
			{
				booleanQuery.Add(new TermQuery(new Term(match.Groups[2].Value, match.Groups[3].Value)), GetOccur(query, match));
				sb.Replace(match.Value, "");
			}
		}

		private static BooleanClause.Occur GetOccur(string query, Match match)
		{
			BooleanClause.Occur occur;
			switch (match.Groups[1].Value)
			{
				case "+":
					occur = BooleanClause.Occur.MUST;
					break;
				case "-":
					occur = BooleanClause.Occur.MUST_NOT;
					break;
				default:
					var followedByAnd = query.Substring(match.Index + match.Length).TrimStart().StartsWith("AND",StringComparison.InvariantCultureIgnoreCase);
					var prefixedByAnd = query.Substring(0, match.Index).TrimEnd().EndsWith("AND", StringComparison.InvariantCultureIgnoreCase);
					var prefixedByNot = query.Substring(0, match.Index).TrimEnd().EndsWith("NOT", StringComparison.InvariantCultureIgnoreCase);

					if(followedByAnd || prefixedByAnd)
						occur = BooleanClause.Occur.MUST;
					else if (prefixedByNot)
						occur = BooleanClause.Occur.MUST_NOT;
					else
						occur = BooleanClause.Occur.SHOULD;

					break;
			}
			return occur;
		}
	}
}