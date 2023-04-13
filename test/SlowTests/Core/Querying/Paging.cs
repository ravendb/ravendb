// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
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
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.Sharded)]
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
        public void BasicPaging_Sharded_Corax(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    RunTestWhenProjectedFieldsAreExtractedFromIndex(store, session.Query<Company>());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.Sharded)]
        public void BasicPaging_Sharded_Lucene_StaticIndexWithStoredField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    RunTestWhenProjectedFieldsAreExtractedFromIndex(store, session.Query<Company, Companies_ByName>());
                }
            }
        }

        public void RunTestWhenProjectedFieldsAreExtractedFromIndex(IDocumentStore store, Raven.Client.Documents.Linq.IRavenQueryable<Company> baseQuery)
        {
            // if all projection fields are extracted from an index then we don't read documents at all
            // in result we don't return @last-modified metadata which is used by orchestrator for default ordering
            // this mean that we cannot have assertions which rely on the insertion order of documents (as it was in the original BasicPaging test)

            // here we're collecting the results from two pages and verify at the end that all results were returned and there are no duplicates

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

                var allCompanies = new List<string>();

                var companies = baseQuery
                    .Customize(x => x.WaitForNonStaleResults())
                    .Statistics(out stats)
                    .Where(c => c.Name.StartsWith("Company"))
                    .Select(c => c.Name)
                    .Distinct()
                    .Take(5)
                    .ToArray();
                Assert.Equal(7, stats.TotalResults);

                Assert.Equal(5, companies.Length);

                allCompanies.AddRange(companies);

                var skipped = stats.SkippedResults;
                companies = baseQuery
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

                allCompanies.AddRange(companies);

                Assert.Equal(6, allCompanies.Count);

                Assert.Contains("Company1", allCompanies);
                Assert.Contains("Company2", allCompanies);
                Assert.Contains("Company3", allCompanies);
                Assert.Contains("Company4", allCompanies);
                Assert.Contains("Company5", allCompanies);
                Assert.Contains("Company6", allCompanies);
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies select new {c.Name};

                Store(x => x.Name, FieldStorage.Yes);
            }
        }
    }
}
