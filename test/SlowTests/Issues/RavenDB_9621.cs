using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9621 : RavenTestBase
    {
        public RavenDB_9621(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MoreLikeThisWithArticialDocumentsContainingArraysShouldNotThrowCastException(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new DataIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Query<Data, DataIndex>()
                        .MoreLikeThis(f => f
                            .UsingDocument("{'Title': 'PR Review', 'Tags': ['reviews', 'perforamnce'] }")
                            .WithOptions(new MoreLikeThisOptions
                            {
                                Fields = new[] { "Body" },
                                MinimumTermFrequency = 1,
                                MinimumDocumentFrequency = 1
                            }))
                        .ToList();
                }
            }
        }

        private class Data
        {
            public string Body { get; set; }
        }

        private class DataIndex : AbstractIndexCreationTask<Data>
        {
            public DataIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Body
                              };

                Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
                {
                    {
                        x => x.Body,
                        typeof(StandardAnalyzer).FullName
                    }
                };

                TermVectors = new Dictionary<Expression<Func<Data, object>>, FieldTermVector>
                {
                    {x => x.Body, FieldTermVector.Yes}
                };
            }
        }
    }
}
