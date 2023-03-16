// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents.Session;
using SlowTests.Issues;
using Tests.Infrastructure;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Querying
{
    public class Paging : RavenTestBase
    {
        public Paging(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void BasicPaging(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Company1" });
                    session.SaveChanges();

                    for (var i = 1; i < 7; i++)
                    {
                        session.Store(new Company { Name = $"Company{i}" });
                        session.SaveChanges();
                    }
                    
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
                    
                    if (options.DatabaseMode == RavenDatabaseMode.Single)
                        Assert.Equal(1, stats.SkippedResults);

                    Assert.Equal(5, companies.Length);
                    Assert.Contains("Company1", companies);
                    Assert.Contains("Company2", companies);
                    Assert.Contains("Company3", companies);
                    Assert.Contains("Company4", companies);
                    Assert.Contains("Company5", companies);

                    var skipped = stats.SkippedResults;
                    companies = session.Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(c => c.Name.StartsWith("Company"))
                        .Select(c => c.Name)
                        .Distinct()
                        .Skip(5 + (int)skipped)
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
