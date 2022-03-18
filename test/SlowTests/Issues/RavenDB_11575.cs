using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11575 : RavenTestBase
    {
        public RavenDB_11575(ITestOutputHelper output) : base(output)
        {
        }

        private class TestIEnumerablesWhereSelectIndex : AbstractIndexCreationTask<MyExternalCooperationDocument, TestIEnumerablesWhereSelectIndex.Info>
        {
            public override string IndexName => "Index1";

            public class Info
            {
                public IEnumerable<string> CountryIds { get; set; }
            }

            public TestIEnumerablesWhereSelectIndex()
            {
                Map = docs => from doc in docs
                              let countries = doc.ForeignPartners != null ? doc.ForeignPartners.Select(x => x.Country) : null
                              select new Info
                              {
                                  CountryIds = countries != null
                                      ? countries.Where(x => x != null).Select(x => x.Id)
                                      : null,
                              };
            }
        }

        #region Document

        private class MyExternalCooperationDocument
        {
            public string Id { get; set; }
            public List<ForeignPartner> ForeignPartners { get; set; }
        }

        private class ForeignPartner
        {
            public Country Country { get; set; }
        }

        private class Country
        {
            public string Id { get; set; }
        }

        #endregion

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                await new TestIEnumerablesWhereSelectIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new MyExternalCooperationDocument
                    {
                        ForeignPartners = new List<ForeignPartner>
                        {
                            new ForeignPartner
                            {
                                Country =new Country
                                {
                                    Id = "1"
                                }
                            }
                        }
                    });

                    session.Store(new MyExternalCooperationDocument());

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TestIEnumerablesWhereSelectIndex.Info, TestIEnumerablesWhereSelectIndex>()
                        .Where(x => x.CountryIds.Contains("1"))
                        .OfType<MyExternalCooperationDocument>()
                        .ToList();

                    Assert.Equal(1, results.Count);

                    results = session.Query<TestIEnumerablesWhereSelectIndex.Info, TestIEnumerablesWhereSelectIndex>()
                        .Where(x => x.CountryIds.Contains("2"))
                        .OfType<MyExternalCooperationDocument>()
                        .ToList();

                    Assert.Equal(0, results.Count);

                    results = session.Query<TestIEnumerablesWhereSelectIndex.Info, TestIEnumerablesWhereSelectIndex>()
                        .Where(x => x.CountryIds == null)
                        .OfType<MyExternalCooperationDocument>()
                        .ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}
