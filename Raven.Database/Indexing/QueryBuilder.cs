using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Util;

namespace Raven.Database.Indexing
{
	public static class QueryBuilder
	{
		static readonly Regex untokenizedQuery = new Regex(@"([+-]?)([\w\d_]+?):`(.+?)`", RegexOptions.Compiled);

		public static Query BuildQuery(string query)
		{
			var matchCollection = untokenizedQuery.Matches(query);
		    var standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
		    try
		    {
                if (matchCollection.Count == 0)
                    return new QueryParser(Version.LUCENE_29, "", standardAnalyzer).Parse(query);
                var sb = new StringBuilder(query);
                var booleanQuery = new BooleanQuery();
                foreach (Match match in matchCollection)
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
                            occur = BooleanClause.Occur.SHOULD;
                            break;
                    }
                    booleanQuery.Add(new TermQuery(new Term(match.Groups[2].Value, match.Groups[3].Value)), occur);
                    sb.Replace(match.Value, "");
                }
                var remaining = sb.ToString().Trim();
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
	}
}