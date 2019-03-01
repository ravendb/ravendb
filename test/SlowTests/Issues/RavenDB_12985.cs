using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12985 : RavenTestBase
    {
        [Fact]
        public void CanUsePagingWhilePatching()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 50; i++)
                    {
                        session.Store(new Company
                        {
                            Name = "C" + i.ToString("D5"),
                            Fax = "F" + i.ToString("D5")
                        }, "companies/" + i.ToString("D5"));
                    }

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation("from Companies as c update { c.Name = 'patch1' } limit 0,2"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                AssertCompanies(store, 0, 2, "patch1");

                operation = store.Operations.Send(new PatchByQueryOperation("from Companies as c update { c.Name = 'patch2' } limit 5,6"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                AssertCompanies(store, 0 + 2 + 5, 6, "patch2"); // previous patch bumped 2 etags

                WaitForIndexing(store);

                operation = store.Operations.Send(new PatchByQueryOperation("from index 'Companies/ByName' as c order by Name update { c.Fax = 'patch3' } limit 7,1"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                AssertCompaniesByIndex(store, 7, 1, "patch3");

                WaitForIndexing(store);

                operation = store.Operations.Send(new PatchByQueryOperation("from index 'Companies/ByName' as c order by Name update { c.Fax = 'patch4' } limit 9,3"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                AssertCompaniesByIndex(store, 9, 3, "patch4");
            }
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
            }
        }

        private static void AssertCompanies(IDocumentStore store, int start, int pageSize, string expected)
        {
            using (var session = store.OpenSession())
            {
                var companies = session.Query<Company>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(50, companies.Count);

                for (int i = 0; i < companies.Count; i++)
                {
                    var company = companies[i];

                    if (i >= start && i < start + pageSize)
                        Assert.Equal(expected, company.Name);
                    else
                        Assert.NotEqual(expected, company.Name);
                }
            }
        }

        private static void AssertCompaniesByIndex(IDocumentStore store, int start, int pageSize, string expected)
        {
            using (var session = store.OpenSession())
            {
                var companies = session.Query<Company>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .OrderBy(x => x.Name)
                    .ToList();

                Assert.Equal(50, companies.Count);

                for (int i = 0; i < companies.Count; i++)
                {
                    var company = companies[i];

                    if (i >= start && i < start + pageSize)
                        Assert.Equal(expected, company.Fax);
                    else
                        Assert.NotEqual(expected, company.Fax);
                }
            }
        }
    }
}
