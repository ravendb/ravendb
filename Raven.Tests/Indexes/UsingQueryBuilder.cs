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
			var query = QueryBuilder.BuildQuery("Name:[[Simple Phrase]]");

			// NOTE: this looks incorrect (looks like Name:Simple Text:Phrase)
			// but internally it is a correct term
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
	}
}
