// -----------------------------------------------------------------------
//  <copyright file="Sorting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;
using Xunit.Abstractions;

using FastTests;
using FastTests.Server.Documents.Indexing;
using Xunit;
using Tests.Infrastructure;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Querying
{
    public class Sorting : RavenTestBase
    {
        public Sorting(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void BasicSorting(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "0Sort" });
                    session.Store(new Company { Name = "Sort1" });
                    session.Store(new Company { Name = "Sort2" });
                    session.SaveChanges();

                    var companies = session.Query<Company>()
                        .OrderBy(c => c.Name)
                        .ToArray();

                    Assert.Equal(3, companies.Length);
                    Assert.Equal("0Sort", companies[0].Name);
                    Assert.Equal("Sort1", companies[1].Name);
                    Assert.Equal("Sort2", companies[2].Name);

                    companies = session.Query<Company>()
                        .OrderByDescending(c => c.Name)
                        .ToArray();

                    Assert.Equal(3, companies.Length);
                    Assert.Equal("Sort2", companies[0].Name);
                    Assert.Equal("Sort1", companies[1].Name);
                    Assert.Equal("0Sort", companies[2].Name);
                }
            }
        }
    }
}
