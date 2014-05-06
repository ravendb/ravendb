using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Database.Indexing;
using Raven.Database.Indexing.LuceneIntegration;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_626 : NoDisposalNeeded
	{
		[Fact]
		public void Mixed()
		{
			var bq = (BooleanQuery)Parse("@in<user>:(users/1,users/2) AND IsActive:true");
			var q = (TermsMatchQuery)bq.Clauses[0].Query;
			var b = (TermQuery)bq.Clauses[1].Query;
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "users/1", "users/2" }, q.Matches);
			Assert.Equal("IsActive", b.Term.Field);
			Assert.Equal("true", b.Term.Text);
		}

		[Fact]
		public void MixedWithSpaces()
		{
			var bq = (BooleanQuery)Parse("@in<user>:(users/1, users/2) AND IsActive:true");
			var q = (TermsMatchQuery)bq.Clauses[0].Query;
			var b = (TermQuery)bq.Clauses[1].Query;
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "users/1", "users/2" }, q.Matches);
			Assert.Equal("IsActive", b.Term.Field);
			Assert.Equal("true", b.Term.Text);
		}

		[Fact]
		public void Simple()
		{
			var q = (TermsMatchQuery)Parse("@in<user>:(users/1,users/2)");
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "users/1", "users/2" }, q.Matches);
		}

		[Fact]
		public void QueryWithSpaces()
		{
			var q = (TermsMatchQuery)Parse("@in<user>:( users/1, users/2)");
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "users/1", "users/2" }, q.Matches);
		}

		[Fact]
		public void QueryWithSpacesAndNot()
		{
			var q = (TermsMatchQuery)Parse("@in<user>:(users/1, users/2)");
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "users/1", "users/2" }, q.Matches);
		}

		[Fact]
		public void QueryWithPhrase()
		{
			var q = (TermsMatchQuery)Parse("@in<user>:(users/1, \"oren eini\")");
			Assert.Equal("user", q.Field);
			Assert.Equal(new[] { "oren eini", "users/1" }, q.Matches);
		}

		private static Query Parse(string q)
		{
			using (var defaultAnalyzer = new KeywordAnalyzer())
			using (var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer))
				return QueryBuilder.BuildQuery(q, perFieldAnalyzerWrapper);
		}
	}
}