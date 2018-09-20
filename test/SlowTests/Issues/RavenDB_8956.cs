using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8956 : RavenTestBase
    {
        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name
                                   };
            }
        }

        [Fact]
        public void OptimizedSortOnlyQuery()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" });
                    session.Store(new Company { Name = "Hibernating Rhinos" });
                    session.Store(new Company { Name = "MS" });
                    session.Store(new Company { Name = "MS" });
                    session.Store(new Company { Name = "Microsoft" });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "CF" });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "IRC" });
                    session.Store(new Company { Name = null });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Z" });
                    session.Store(new Company { Name = "ZXZ" });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                QueryStatistics allStatistics;
                List<string> allCompanies;
                using (var session = store.OpenSession())
                {
                    allCompanies = session.Query<Company, Companies_ByName>()
                        .Statistics(out allStatistics)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name != Guid.NewGuid().ToString())
                        .OrderBy(x => x.Name)
                        .Select(x => x.Name)
                        .ToList();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Select(x => x.Name)
                        .ToList();
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(allCompanies.OrderBy(x => x), companies);
                    Assert.Equal(allStatistics.TotalResults, stats.TotalResults);

                    companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.Name)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(allCompanies.OrderByDescending(x => x), companies);
                    Assert.Equal(allStatistics.TotalResults, stats.TotalResults);
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Select(x => x.Name)
                        .Skip(1)
                        .Take(3)
                        .ToList();

                    Assert.Equal(allCompanies.OrderBy(x => x).Skip(1).Take(3), companies);
                    Assert.Equal(allStatistics.TotalResults, stats.TotalResults);

                    companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.Name)
                        .Select(x => x.Name)
                        .Skip(2)
                        .Take(4)
                        .ToList();

                    Assert.Equal(allCompanies.OrderByDescending(x => x).Skip(2).Take(4), companies);
                    Assert.Equal(allStatistics.TotalResults, stats.TotalResults);
                }
            }
        }
    }
}
