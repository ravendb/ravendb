using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Raven.Database.Indexing;

namespace Raven.Database.FileSystem.Search
{
	public class SimpleFilesQueryParser : QueryParser
	{
		private readonly HashSet<string> numericFields;

		public SimpleFilesQueryParser(Analyzer analyzer, IEnumerable<string> numericFields)
            : base(Version.LUCENE_29, string.Empty, analyzer)
		{
			this.numericFields = new HashSet<string>(numericFields);
		}

		protected override Query NewRangeQuery(string field, string part1, string part2, bool inclusive)
		{
			if (numericFields.Contains(field))
			{
				long lower;
				long upper;

				if (!long.TryParse(part1, out lower))
					lower = long.MinValue;

				if (!long.TryParse(part2, out upper))
					upper = long.MaxValue;

				var rangeQuery = NumericRangeQuery.NewLongRange(field, lower, upper, inclusive, inclusive);

				return rangeQuery;
			}

			return base.NewRangeQuery(field, part1, part2, inclusive);
		}

        public override Query Parse(string originalQuery)
        {
            var query = originalQuery;
            query = QueryBuilder.PreProcessComments(query);
            query = QueryBuilder.PreProcessMixedInclusiveExclusiveRangeQueries(query);
            query = QueryBuilder.PreProcessSearchTerms(query);

            var generatedQuery = base.Parse(query);
            generatedQuery = HandleMethods(generatedQuery);

            return generatedQuery;
        }

        private static Query HandleMethods(Query query)
        {
            var termQuery = query as TermQuery;
            if (termQuery != null && termQuery.Term.Field.StartsWith("@"))
            {
                return QueryBuilder.HandleMethodsForQueryAndTerm(query, termQuery.Term);
            }
            var pharseQuery = query as PhraseQuery;
            if (pharseQuery != null)
            {
                var terms = pharseQuery.GetTerms();
                if (terms.All(x => x.Field.StartsWith("@")) == false ||
                    terms.Select(x => x.Field).Distinct().Count() != 1)
                    return query;
                return QueryBuilder.HandleMethodsForQueryAndTerm(query, terms);
            }
            var wildcardQuery = query as WildcardQuery;
            if (wildcardQuery != null)
            {
                return QueryBuilder.HandleMethodsForQueryAndTerm(query, wildcardQuery.Term);
            }
           
            return query;
        }
	}
}