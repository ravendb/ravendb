using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Nest;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18766 : RavenTestBase
{
    public RavenDB_18766(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Querying)]
    public void ShouldThrowOnAttemptToCreateIndexWithOutputReduce()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var e = Assert.Throws<NotSupportedInShardingException>(() =>
            {
                using (var session = store.OpenSession())
                {
                    var list = session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == "data/1").WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" }
                        }))
                        .ToList();
                }
            });
            Assert.Contains("MoreLikeThis queries are currently not supported in a sharded database ", e.Message);
        }
    }

    private class Data
    {
        public string Id { get; set; }
        public string Body { get; set; }
        public string WhitespaceAnalyzerField { get; set; }
        public string PersonId { get; set; }
    }

    private class DataIndex : AbstractIndexCreationTask<Data>
    {
        public DataIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Body, doc.WhitespaceAnalyzerField };

            Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
            {
                {
                    x => x.Body,
                    typeof (StandardAnalyzer).FullName
                },
            };

            TermVectors = new Dictionary<Expression<Func<Data, object>>, FieldTermVector>
            {
                {
                    x => x.Body, FieldTermVector.Yes
                },
                {
                    x => x.WhitespaceAnalyzerField, FieldTermVector.Yes
                }
            };
        }
    }
}
