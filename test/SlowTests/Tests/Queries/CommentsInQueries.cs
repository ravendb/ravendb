// -----------------------------------------------------------------------
//  <copyright file="CommentsInQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Lucene.Net.Analysis;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries;
using Xunit;

namespace SlowTests.Tests.Queries
{
    public class CommentsInQueries : NoDisposalNeeded
    {
        [Fact]
        public void ShouldBeSafelyIgnored()
        {
            var query = QueryBuilder.BuildQuery(@"Hi: There mister // comment

Hi: ""where // are "" // comment

Be: http\://localhost\:8080

", new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer()));

            var s = query.ToString();

            Assert.DoesNotContain("comment", s);
        }
    }
}
