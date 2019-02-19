using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
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
        public void CanSearchWithPrefixWildcard(string query)
        {
            using (var store = GetDocumentStore())
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
                        .Customize(customization => customization.WaitForNonStaleResults());
                    var result =
                        rq.Search(x => x.Query, query)
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
                Indexes.Add(x => x.Query, FieldIndexing.Search);
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
