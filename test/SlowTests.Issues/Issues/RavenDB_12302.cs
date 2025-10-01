using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12302 : RavenTestBase
    {
        public RavenDB_12302(ITestOutputHelper output) : base(output)
        {
        }

        private class Companies_ByNameExact : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string[] Names { get; set; }
            }

            public Companies_ByNameExact()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Names = c.Name
                                   };

                Index("Names", FieldIndexing.Exact);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Companies_ByNameExact().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Hibernating Rhinos"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var company = session
                        .Query<Companies_ByNameExact.Result, Companies_ByNameExact>()
                        .Where(x => x.Names.Contains("Hibernating Rhinos"))
                        .OfType<Company>()
                        .FirstOrDefault();

                    Assert.NotNull(company);

                    company = session
                        .Query<Companies_ByNameExact.Result, Companies_ByNameExact>()
                        .Where(x => x.Names.In(new[] { "Hibernating Rhinos" }))
                        .OfType<Company>()
                        .FirstOrDefault();

                    Assert.NotNull(company);

                    company = session
                        .Advanced
                        .RawQuery<Company>("from index 'Companies/ByNameExact' where Names between 'Hibernating Rhinos' and 'Hibernating Rhinos'")
                        .FirstOrDefault();

                    Assert.NotNull(company);
                }
            }
        }
    }
}
