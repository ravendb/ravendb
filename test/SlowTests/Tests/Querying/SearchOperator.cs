using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Querying
{
    public class SearchOperator : RavenTestBase
    {
        public SearchOperator(ITestOutputHelper output) : base(output)
        {
        }

        private class Something
        {
            public string Id { get; set; }
            public string MyProp { get; set; }
        }

        private class FTSIndex : AbstractIndexCreationTask<Something>
        {
            public FTSIndex()
            {
                Map = docs => from doc in docs
                              select new { doc.MyProp };

                Indexes.Add(x => x.MyProp, FieldIndexing.Search);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void DynamicLuceneQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new FTSIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // insert two test documents
                    session.Store(new Something { Id = "23", MyProp = "the first string contains misspelled word sofware" });
                    session.Store(new Something { Id = "34", MyProp = "the second string contains the word software" });
                    session.SaveChanges();

                    // search for the keyword software
                    var results = session.Advanced.DocumentQuery<Something>("FTSIndex").WhereLucene("MyProp", "software")
                        .WaitForNonStaleResults()
                        .ToList();
                    Assert.Equal(1, results.Count);

                    results = session.Advanced.DocumentQuery<Something>("FTSIndex").WhereLucene("MyProp", "software~")
                        .WaitForNonStaleResults()
                        .ToList();
                    Assert.Equal(2, results.Count);
                }
            }
        }
    }
}
