// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Querying
{
    public class Paging : RavenTestBase
    {
        [Fact]
        public void BasicPaging()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Company1" });
                    session.Store(new Company { Name = "Company1" });
                    session.Store(new Company { Name = "Company2" });
                    session.Store(new Company { Name = "Company3" });
                    session.Store(new Company { Name = "Company4" });
                    session.Store(new Company { Name = "Company5" });
                    session.Store(new Company { Name = "Company6" });
                    session.Store(new Company { Name = "ompany7" });
                    session.SaveChanges();

                    QueryStatistics stats;

                    var companies = session.Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(c => c.Name.StartsWith("Company"))
                        .Select(c => c.Name)
                        .Distinct()
                        .Take(5)
                        .ToArray();
                    Assert.Equal(7, stats.TotalResults);
                    Assert.Equal(1, stats.SkippedResults);
                    Assert.Equal(5, companies.Length);
                    Assert.Equal("Company1", companies[0]);
                    Assert.Equal("Company2", companies[1]);
                    Assert.Equal("Company3", companies[2]);
                    Assert.Equal("Company4", companies[3]);
                    Assert.Equal("Company5", companies[4]);

                    var skipped = stats.SkippedResults;
                    companies = session.Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(c => c.Name.StartsWith("Company"))
                        .Select(c => c.Name)
                        .Distinct()
                        .Skip(5 + skipped)
                        .Take(5)
                        .ToArray();
                    Assert.Equal(7, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Company6", companies[0]);
                }
            }
        }
    }
}
