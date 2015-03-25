using System;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.FileSystem.Search
{
	public class SimpleFilesQueryParser : QueryParser
	{
		public static readonly Regex NumericRangeValue = new Regex(@"^[\w\d]x[-\w\d.]+$", RegexOptions.Compiled);
		public static readonly Regex DateTimeValue = new Regex(@"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z?)", RegexOptions.Compiled);

        private readonly RavenPerFieldAnalyzerWrapper fieldAnalyzer;

		public SimpleFilesQueryParser(Analyzer analyzer)
            : base(Version.LUCENE_29, string.Empty, analyzer)
		{
            fieldAnalyzer = new RavenPerFieldAnalyzerWrapper(fieldAnalyzer);
		}

		protected override Query GetRangeQuery(string field, string lower, string upper, bool inclusive)
		{
			bool minInclusive = inclusive;
			bool maxInclusive = inclusive;
			if (lower == "NULL" || lower == "*")
			{
				lower = null;
				minInclusive = true;
			}
			if (upper == "NULL" || upper == "*")
			{
				upper = null;
				maxInclusive = true;
			}

			if ((lower == null || !NumericRangeValue.IsMatch(lower)) && (upper == null || !NumericRangeValue.IsMatch(upper)))
			{
				return NewRangeQuery(field, lower, upper, inclusive);
			}

			var from = NumberUtil.StringToNumber(lower);
			var to = NumberUtil.StringToNumber(upper);

			TypeCode numericType;

			if (from != null)
				numericType = Type.GetTypeCode(from.GetType());
			else if (to != null)
				numericType = Type.GetTypeCode(to.GetType());
			else
				numericType = TypeCode.Empty;

			switch (numericType)
			{
				case TypeCode.Int64:
				case TypeCode.Int32:
				{
					var fromLong = from as long?;
					var fromInt = from as int?;

					var toLong = to as long?;
					var toInt = to as int?;

					return NumericRangeQuery.NewLongRange(field, fromLong ?? fromInt ?? Int64.MinValue, toLong ?? toInt ?? Int64.MaxValue, minInclusive, maxInclusive);
				}
				case TypeCode.Single:
				case TypeCode.Double:
				{
					var fromDouble = from as double?;
					var fromFloat = from as float?;

					var toDouble = to as double?;
					var toFloat = to as float?;

					return NumericRangeQuery.NewDoubleRange(field, fromDouble ?? fromFloat ?? Double.MinValue, toDouble ?? toFloat ?? Double.MaxValue, minInclusive, maxInclusive);
				}
				default:
				{
					return NewRangeQuery(field, lower, upper, inclusive);
				}
			}
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