using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Bugs.Errors
{
    public class QueryIssues : RavenTestBase
    {
        public QueryIssues(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]    
        public void PrestonThinksLoadStartingWithShouldBeCaseInsensitive(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new CompanyIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = "CoMpAnY/1", Name = "This Company", HasParentCompany = false });
                    session.Store(new Company { Id = "CoMpAnY/2", Name = "That Company", HasParentCompany = true });
                    session.Store(new Company { Id = "CoMpAnY/3", Name = "The Other Company", HasParentCompany = false });
                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    //first query to make sure we aren't stale.
                    session.Query<Company>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    var loadResult = session.Load<Company>("cOmPaNy/1");
                    var loadStartResults = session.Advanced.LoadStartingWith<Company>("cOmPaNy/1").ToList();
                    Assert.Contains(loadResult, loadStartResults);
                    Assert.Equal(1, loadStartResults.Count);
                }
            }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public bool HasParentCompany { get; set; }
        }

        private class CompanyIndex : AbstractIndexCreationTask<Company>
        {
            public CompanyIndex()
            {
                Map = companies =>
                    from company in companies
                    select new
                    {
                        company.Id,
                        company.Name,
                        company.HasParentCompany
                    };
            }
        }

    }
}
