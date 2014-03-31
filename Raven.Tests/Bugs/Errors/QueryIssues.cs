using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Raven.Tests.Common;

using Xunit;
using Raven.Client;


namespace Raven.Tests.Bugs.Errors
{
    public class QueryIssues : RavenTest
    {
        [Fact]
        public void PrestonThinksLoadStartingWithShouldBeCaseInsensitive()
        {
            using (var store = NewDocumentStore())
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
                    session.Query<Company>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();

                    var loadResult = session.Load<Company>("cOmPaNy/1");
                    var loadStartResults = session.Advanced.LoadStartingWith<Company>("cOmPaNy/1").ToList();
                    Assert.Contains(loadResult, loadStartResults);
                    Assert.Equal(1, loadStartResults.Count);
                }
            }
        }

        public class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public bool HasParentCompany { get; set; }
        }

        public class CompanyIndex : AbstractIndexCreationTask<Company>
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
