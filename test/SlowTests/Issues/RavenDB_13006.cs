using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13006 : RavenTestBase
    {
        public RavenDB_13006(ITestOutputHelper output) : base(output)
        {
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };

                Analyze(x => x.Name, "KeywordAnalyzer");
            }
        }

        [Fact]
        public void CanUseExact()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = " My Company "
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name == " My Company ")
                        .ToList();

                    Assert.Equal(0, companies.Count);

                    companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name == " My Company ", exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);

                    companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name.StartsWith(" My Company"))
                        .ToList();

                    Assert.Equal(0, companies.Count);

                    companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name.StartsWith(" My Company"), exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);

                    companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name.EndsWith("My Company "))
                        .ToList();

                    Assert.Equal(0, companies.Count);

                    companies = session.Query<Company, Companies_ByName>()
                        .Where(x => x.Name.EndsWith("My Company "), exact: true)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                }
            }
        }
    }
}
