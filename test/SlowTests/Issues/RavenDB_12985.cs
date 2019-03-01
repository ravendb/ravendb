using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12985 : RavenTestBase
    {
        [Fact]
        public void CanUsePagingWhilePatchingOrDeleting()
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

                AssertDelete(store, 3, 5, 50);

                AssertDelete(store, 7, 13, 45);

                AssertDeleteByIndex(store, 9, 2, 32);

                AssertDeleteByIndex(store, 6, 23, 30);
            }
        }

        private static void AssertDelete(IDocumentStore store, int start, int take, int count)
        {
            using (var session = store.OpenSession(new SessionOptions
            {
                NoTracking = true,
                NoCaching = true
            }))
            {
                var preCompanies = session.Query<Company>().Select(x => x.Id).ToList();
                Assert.Equal(count, preCompanies.Count);

                var operation = store.Operations.Send(new DeleteByQueryOperation($"from Companies limit {start},{take}"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                var postCompanies = session.Query<Company>().Select(x => x.Id).ToList();
                Assert.Equal(count - take, postCompanies.Count);

                var idsThatShouldBeDeleted = preCompanies.Skip(start).Take(take).ToList();
                foreach (var id in idsThatShouldBeDeleted)
                    Assert.DoesNotContain(id, postCompanies);
            }
        }

        private static void AssertDeleteByIndex(IDocumentStore store, int start, int take, int count)
        {
            using (var session = store.OpenSession(new SessionOptions
            {
                NoTracking = true,
                NoCaching = true
            }))
            {
                WaitForIndexing(store);

                var preCompanies = session.Query<Company>().OrderBy(x => x.Name).Select(x => x.Id).ToList();
                Assert.Equal(count, preCompanies.Count);

                var operation = store.Operations.Send(new DeleteByQueryOperation($"from index 'Companies/ByName' order by Name limit {start},{take}"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                WaitForIndexing(store);

                var postCompanies = session.Query<Company>().OrderBy(x => x.Name).Select(x => x.Id).ToList();
                Assert.Equal(count - take, postCompanies.Count);

                var idsThatShouldBeDeleted = preCompanies.Skip(start).Take(take).ToList();
                foreach (var id in idsThatShouldBeDeleted)
                    Assert.DoesNotContain(id, postCompanies);
            }
        }

        private static void AssertCompanies(IDocumentStore store, int start, int take, string expected)
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

                    if (i >= start && i < start + take)
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
    }
}
