using System;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class UsingQueryBuilder
	{
		[Fact]
		public void Can_parse_Analyzed_simple_single_term()
		{
			var query = QueryBuilder.BuildQuery("Name:SingleTerm");

			Assert.Equal("Name:singleterm", query.ToString());
		}

		[Fact]
		public void Can_parse_Analyzed_simple_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:\"Simple Phrase\"");

			Assert.Equal("Name:\"simple phrase\"", query.ToString());
		}

		[Fact]
		public void Can_parse_Analyzed_escaped_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:\"Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase\"");

			Assert.Equal("Name:\"escaped phrase\"", query.ToString());
		}

		[Fact]
		public void Can_parse_NotAnalyzed_simple_single_term()
		{
			var query = QueryBuilder.BuildQuery("Name:[[SingleTerm]]");

			Assert.Equal("Name:SingleTerm", query.ToString());
		}

		[Fact]
		public void Can_parse_NotAnalyzed_simple_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:[[\"Simple Phrase\"]]");

			// NOTE: this looks incorrect (looks like "Name:Simple OR DEFAULT_FIELD:Phrase") but internally it is a single phrase
			Assert.Equal("Name:Simple Phrase", query.ToString());
		}

		[Fact]
		public void Can_parse_NotAnalyzed_escaped_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:[[Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase]]");

			// QueryBuilder should know how to properly unescape
			Assert.Equal("Name:Escaped+-&|!(){}[]^\"~*?:\\Phrase", query.ToString());
		}

		[Fact]
		public void Can_parse_LessThan_on_date()
		{
			var query = QueryBuilder.BuildQuery("Birthday:{NULL TO 20100515000000000}");

			Assert.Equal("Birthday:{null TO 20100515000000000}", query.ToString());
		}

		[Fact]
		public void Can_parse_LessThanOrEqual_on_date()
		{
			var query = QueryBuilder.BuildQuery("Birthday:[NULL TO 20100515000000000]");

			Assert.Equal("Birthday:[null TO 20100515000000000]", query.ToString());
		}

		[Fact]
		public void Can_parse_GreaterThan_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:{0x00000003 TO NULL}");

			Assert.Equal("Age_Range:{3 TO 2147483647}", query.ToString());
		}

		[Fact]
		public void Can_parse_GreaterThanOrEqual_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:[0x00000003 TO NULL]");

			Assert.Equal("Age_Range:[3 TO 2147483647]", query.ToString());
		}

		[Fact]
		public void Can_parse_fixed_range_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:{0x00000003 TO 0x00000009}");

			Assert.Equal("Age_Range:{3 TO 9}", query.ToString());
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" AND Name:SingleTerm) OR (Age:3 AND Birthday:20100515000000000)");

			Assert.Equal("(+Name:\"simple phrase\" +Name:singleterm) (+Age:3 +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" OR Name:SingleTerm) AND (Age:3 OR Birthday:20100515000000000)");

			Assert.Equal("+(Name:\"simple phrase\" Name:singleterm) +(Age:3 Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query_with_int_range()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" AND Name:SingleTerm) OR (Age_Range:{0x00000003 TO NULL} AND Birthday:20100515000000000)");

			Assert.Equal("(+Name:\"simple phrase\" +Name:singleterm) (+Age_Range:{3 TO 2147483647} +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query_with_date_range()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" OR Name:SingleTerm) AND (Age_Range:3 OR Birthday:[NULL TO 20100515000000000])");

			Assert.Equal("+(Name:\"simple phrase\" Name:singleterm) +(Age_Range:3 Birthday:[null TO 20100515000000000])", query.ToString());
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query_with_NotAnalyzed_field()
		{
			var query = QueryBuilder.BuildQuery("(AnalyzedName:\"Simple Phrase\" AND NotAnalyzedName:[[\"Simple Phrase\"]]) OR (Age_Range:3 AND Birthday:20100515000000000)");

			// NOTE: this looks incorrect (looks like "Name:Simple OR DEFAULT_FIELD:Phrase") but internally it is a single phrase
			Assert.Equal("(+AnalyzedName:\"simple phrase\" +NotAnalyzedName:Simple Phrase) (+Age_Range:3 +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query_with_escaped_field()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase\" OR Name:SingleTerm) AND (Age_Range:3 OR Birthday:20100515000000000)");

			Assert.Equal("+(Name:\"escaped phrase\" Name:singleterm) +(Age_Range:3 Birthday:20100515000000000)", query.ToString());
		}
	}
}
