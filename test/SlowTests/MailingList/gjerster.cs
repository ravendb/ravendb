using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class gjerster : RavenTestBase
    {
        [Theory]
        [InlineData("singa*")]
        [InlineData("pte")]
        [InlineData("ltd")]
        [InlineData("*inga*")]
        public async Task CanSearchWithPrefixWildcard(string query)
        {
            using (var store = await GetDocumentStore())
            {
                new SampleDataIndex().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new SampleData
                    {
                        Name = "Singapore",
                        Description = "SINGAPORE PTE LTD"
                    });

                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    var rq = session
                        .Query<SampleDataIndex.ReducedResult, SampleDataIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow());
                    var result =
                        rq.Search(x => x.Query, query,
                                  escapeQueryOptions: EscapeQueryOptions.AllowAllWildcards)
                            .As<SampleData>()
                            .Take(10)
                            .ToList();
                    if (result.Count == 0)
                    {

                    }
                    Assert.NotEmpty(result);
                }
            }
        }

        private class SampleData
        {
            public string Name { get; set; }
            public string Description { get; set; }
        }

        private class SampleDataIndex : AbstractIndexCreationTask<SampleData, SampleDataIndex.ReducedResult>
        {
            public SampleDataIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Query = new object[]
                                  {
                                  doc.Name,
                                  doc.Description
                                  }
                              };
                Indexes.Add(x => x.Query, FieldIndexing.Analyzed);
            }

            #region Nested type: ReducedResult

            public class ReducedResult
            {
                public string Query { get; set; }
            }

            #endregion
        }
    }
}
