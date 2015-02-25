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
        private readonly RavenPerFieldAnalyzerWrapper fieldAnalyzer;

		public SimpleFilesQueryParser(Analyzer analyzer, IEnumerable<string> numericFields)
            : base(Version.LUCENE_29, string.Empty, analyzer)
		{
			this.numericFields = new HashSet<string>(numericFields);
            this.fieldAnalyzer = new RavenPerFieldAnalyzerWrapper(fieldAnalyzer);
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
            generatedQuery = QueryBuilder.HandleMethods(generatedQuery, fieldAnalyzer);

            return generatedQuery;
        }   
	}
}