using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12030 : RavenTestBase
    {
        private class Fox
        {
            public string Name { get; set; }
        }

        private class Fox_Search : AbstractIndexCreationTask<Fox>
        {
            public Fox_Search()
            {
                Map = foxes => from f in foxes
                               select new
                               {
                                   Name = f.Name
                               };

                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        [Fact]
        public void SimpleFuzzy()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Hibernating Rhinos"
                    });

                    session.Store(new Company
                    {
                        Name = "CodeForge"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "CoedForhe").Fuzzy(0.5m)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("CodeForge", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "Hiberanting Rinhos").Fuzzy(0.5m)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("Hibernating Rhinos", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "CoedForhe").Fuzzy(0.99m)
                        .ToList();

                    Assert.Equal(0, companies.Count);
                }
            }
        }

        [Fact]
        public void SimpleProximity()
        {
            using (var store = GetDocumentStore())
            {
                new Fox_Search().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Fox
                    {
                        Name = "a quick brown fox"
                    });

                    session.Store(new Fox
                    {
                        Name = "the fox is quick"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var foxes = session
                        .Advanced
                        .DocumentQuery<Fox, Fox_Search>()
                        .Search(x => x.Name, "quick fox").Proximity(1)
                        .ToList();

                    Assert.Equal(1, foxes.Count);
                    Assert.Equal("a quick brown fox", foxes[0].Name);

                    foxes = session
                        .Advanced
                        .DocumentQuery<Fox, Fox_Search>()
                        .Search(x => x.Name, "quick fox").Proximity(2)
                        .ToList();

                    Assert.Equal(2, foxes.Count);
                    Assert.Equal("a quick brown fox", foxes[0].Name);
                    Assert.Equal("the fox is quick", foxes[1].Name);
                }
            }
        }
    }
}
