// -----------------------------------------------------------------------
//  <copyright file="CommentsInQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lucene.Net.Analysis;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Queries
{
	public class CommentsInQueries
	{
		[Fact]
		public void ShouldBeSafelyIgnored()
		{
			var query = QueryBuilder.BuildQuery(@"Hi: There mister // comment

Hi: ""where // are "" // comment

Be: http\://localhost\:8080

",new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer()));

			var s = query.ToString();

			Assert.DoesNotContain("comment", s);
		}
	}
}