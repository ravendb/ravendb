using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class SearchOperator : RavenTestBase
    {
        private class Something
        {
            public int Id { get; set; }
            public string MyProp { get; set; }
        }

        private class FTSIndex : AbstractIndexCreationTask<Something>
        {
            public FTSIndex()
            {
                Map = docs => from doc in docs
                              select new { doc.MyProp };

                Indexes.Add(x => x.MyProp, FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public async Task DynamicLuceneQuery()
        {
            using (var store = await GetDocumentStore())
            {
                new FTSIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // insert two test documents
                    session.Store(new Something { Id = 23, MyProp = "the first string contains misspelled word sofware" });
                    session.Store(new Something { Id = 34, MyProp = "the second string contains the word software" });
                    session.SaveChanges();

                    // search for the keyword software
                    var results = session.Advanced.DocumentQuery<Something>("FTSIndex").Search("MyProp", "software")
                        .WaitForNonStaleResultsAsOfLastWrite()
                        .ToList();
                    Assert.Equal(1, results.Count);

                    results = session.Advanced.DocumentQuery<Something>("FTSIndex").Search("MyProp", "software~")
                        .WaitForNonStaleResultsAsOfLastWrite().ToList();
                    Assert.Equal(2, results.Count);
                }
            }
        }
    }
}
