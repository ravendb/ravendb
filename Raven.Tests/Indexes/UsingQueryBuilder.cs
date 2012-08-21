//-----------------------------------------------------------------------
// <copyright file="UsingQueryBuilder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using System.Globalization;

using Lucene.Net.Search;
using Raven.Database.Indexing;
using Xunit;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tests.Indexes
{
	public class UsingQueryBuilder
	{
		[Fact]
		public void Can_parse_Analyzed_simple_single_term()
		{
			var query = QueryBuilder.BuildQuery("Name:SingleTerm", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:singleterm", query.ToString());
			Assert.True(query is TermQuery);
			Assert.Equal(((TermQuery)query).Term.Field, "Name");
			Assert.Equal(((TermQuery)query).Term.Text, "singleterm");
		}

		[Fact]
		public void Can_parse_Analyzed_simple_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:\"Simple Phrase\"", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:\"simple phrase\"", query.ToString());
			Assert.True(query is PhraseQuery);
			var terms = ((PhraseQuery)query).GetTerms();
			Assert.Equal(terms[0].Field, "Name");
			Assert.Equal(terms[0].Text, "simple");
			Assert.Equal(terms[1].Field, "Name");
			Assert.Equal(terms[1].Text, "phrase");
		}

		[Fact]
		public void Can_parse_Analyzed_escaped_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:\"Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase\"", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:\"escaped phrase\"", query.ToString());
			Assert.True(query is PhraseQuery);
			var terms = ((PhraseQuery)query).GetTerms();
			Assert.Equal(terms[0].Field, "Name");
			Assert.Equal(terms[0].Text, "escaped");
			Assert.Equal(terms[1].Field, "Name");
			Assert.Equal(terms[1].Text, "phrase");
		}

		[Fact]
		public void Can_parse_NotAnalyzed_simple_single_term()
		{
			var query = QueryBuilder.BuildQuery("Name:[[SingleTerm]]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:SingleTerm", query.ToString());
			Assert.True(query is TermQuery);
			Assert.Equal(((TermQuery)query).Term.Field, "Name");
			Assert.Equal(((TermQuery)query).Term.Text, "SingleTerm");
		}

		[Fact]
		public void Can_parse_NotAnalyzed_simple_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:[[\"Simple Phrase\"]]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:Simple Phrase", query.ToString());
			Assert.True(query is TermQuery);
			Assert.Equal(((TermQuery)query).Term.Field, "Name");
			Assert.Equal(((TermQuery)query).Term.Text, "Simple Phrase");
		}

		[Fact]
		public void Can_parse_NotAnalyzed_escaped_phrase()
		{
			var query = QueryBuilder.BuildQuery("Name:[[Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase]]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Name:Escaped+-&|!(){}[]^\"~*?:\\Phrase", query.ToString());
			Assert.True(query is TermQuery);
			Assert.Equal(((TermQuery)query).Term.Field, "Name");
			Assert.Equal(((TermQuery)query).Term.Text, "Escaped+-&|!(){}[]^\"~*?:\\Phrase");
		}

		[Fact]
		public void Can_parse_LessThan_on_date()
		{
			var query = QueryBuilder.BuildQuery("Birthday:{NULL TO 20100515000000000}", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Birthday:{* TO 20100515000000000}", query.ToString());
		}

		[Fact]
		public void Can_parse_LessThanOrEqual_on_date()
		{
			var query = QueryBuilder.BuildQuery("Birthday:[NULL TO 20100515000000000]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Birthday:[* TO 20100515000000000]", query.ToString());
		}

		[Fact]
		public void Can_parse_GreaterThan_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:{0x00000003 TO NULL}", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Age_Range:{3 TO 2147483647}", query.ToString());
			Assert.True(query is NumericRangeQuery<int>);
		}

		[Fact]
		public void Can_parse_GreaterThanOrEqual_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:[0x00000003 TO NULL]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Age_Range:[3 TO 2147483647]", query.ToString());
			Assert.True(query is NumericRangeQuery<int>);
		}

		[Fact]
		public void Can_parse_GreaterThanOrEqual_on_long()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:[0x0000000000000003 TO NULL]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Age_Range:[3 TO 9223372036854775807]", query.ToString());
			Assert.True(query is NumericRangeQuery<long>);
		}

		[Fact]
		public void Can_parse_GreaterThanOrEqual_on_double()
		{
			var query = QueryBuilder.BuildQuery("Price_Range:[Dx1.0 TO NULL]", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Price_Range:[1 TO 1" + CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator + "79769313486232E+308]", query.ToString());
			Assert.True(query is NumericRangeQuery<double>);
		}

		[Fact]
		public void Can_parse_LessThan_on_float()
		{
			var query = QueryBuilder.BuildQuery("Price_Range:{NULL TO Fx1.0}", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("Price_Range:{-3" + CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator + "402823E+38 TO 1}", query.ToString());
			Assert.True(query is NumericRangeQuery<float>);
		}

		[Fact]
		public void Can_parse_fixed_range_on_int()
		{
			var query = QueryBuilder.BuildQuery("Age_Range:{0x00000003 TO 0x00000009}", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_30)));

			Assert.Equal("Age_Range:{3 TO 9}", query.ToString());
			Assert.True(query is NumericRangeQuery<int>);
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" AND Name:SingleTerm) OR (Age:3 AND Birthday:20100515000000000)", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("(+Name:\"simple phrase\" +Name:singleterm) (+Age:3 +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" OR Name:SingleTerm) AND (Age:3 OR Birthday:20100515000000000)", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("+(Name:\"simple phrase\" Name:singleterm) +(Age:3 Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query_with_int_range()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" AND Name:SingleTerm) OR (Age_Range:{0x00000003 TO NULL} AND Birthday:20100515000000000)", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("(+Name:\"simple phrase\" +Name:singleterm) (+Age_Range:{3 TO 2147483647} +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query_with_date_range()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Simple Phrase\" OR Name:SingleTerm) AND (Age_Range:3 OR Birthday:[NULL TO 20100515000000000])", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("+(Name:\"simple phrase\" Name:singleterm) +(Age_Range:3 Birthday:[* TO 20100515000000000])", query.ToString());
		}

		[Fact]
		public void Can_parse_conjunctions_within_disjunction_query_with_NotAnalyzed_field()
		{
			var query = QueryBuilder.BuildQuery("(AnalyzedName:\"Simple Phrase\" AND NotAnalyzedName:[[\"Simple Phrase\"]]) OR (Age_Range:3 AND Birthday:20100515000000000)", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			// NOTE: this looks incorrect (looks like "Name:Simple OR DEFAULT_FIELD:Phrase") but internally it is a single phrase
			Assert.Equal("(+AnalyzedName:\"simple phrase\" +NotAnalyzedName:Simple Phrase) (+Age_Range:3 +Birthday:20100515000000000)", query.ToString());
		}

		[Fact]
		public void Can_parse_disjunctions_within_conjunction_query_with_escaped_field()
		{
			var query = QueryBuilder.BuildQuery("(Name:\"Escaped\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\Phrase\" OR Name:SingleTerm) AND (Age_Range:3 OR Birthday:20100515000000000)", new PerFieldAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_29)));

			Assert.Equal("+(Name:\"escaped phrase\" Name:singleterm) +(Age_Range:3 Birthday:20100515000000000)", query.ToString());
		}
	}
}
