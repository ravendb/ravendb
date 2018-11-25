using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12223 : RavenTestBase
    {
        private class Search_Whitespace : AbstractIndexCreationTask<Company>
        {
            public Search_Whitespace()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name
                                   };

                Index(x => x.Name, FieldIndexing.Search);
                Analyze(x => x.Name, "WhitespaceAnalyzer");
            }
        }

        [Fact]
        public void CanUsePhrases()
        {
            using (var store = GetDocumentStore())
            {
                new Search_Whitespace().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Hibernating"
                    });

                    session.Store(new Company
                    {
                        Name = "Hibernating Rhinos"
                    });

                    session.Store(new Company
                    {
                        Name = "Hibernating\" Rhinos"
                    });

                    session.Store(new Company
                    {
                        Name = "Rhinos"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company, Search_Whitespace>()
                        .Search(x => x.Name, "\"Hibernating\\\" Rhinos\"")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("Hibernating\" Rhinos", companies[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company>()
                        .Search(x => x.Name, "\"Hibernating Rhinos\"")
                        .ToList();

                    Assert.Equal(2, companies.Count);
                    Assert.True(companies.Any(x => x.Name == "Hibernating Rhinos"));
                    Assert.True(companies.Any(x => x.Name == "Hibernating\" Rhinos"));
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company>()
                        .Search(x => x.Name, "Hibernating Rhinos")
                        .ToList();

                    Assert.Equal(4, companies.Count);
                }
            }
        }
    }
}
