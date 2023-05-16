// -----------------------------------------------------------------------
//  <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Tests.Infrastructure;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Querying
{
    public class Projections : RavenTestBase
    {
        public Projections(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void BasicProjections(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Some Company 1", Address1 = "Address1" });
                    session.Store(new Company { Name = "Some Company 2", Address2 = "Address2" });
                    session.Store(new Company { Name = "ASome Company 3", Address3 = "Address3" });
                    session.SaveChanges();

                    var anonymousCompanyNames = (from company in session.Query<Company>()
                                                 where company.Name.StartsWith("Some")
                                                 select new { company.Name })
                                       .ToArray();

                    Assert.Equal(2, anonymousCompanyNames.Length);
                    Assert.Equal("Some Company 1", anonymousCompanyNames[0].Name);
                    Assert.Equal("Some Company 2", anonymousCompanyNames[1].Name);

                    Company[] companyNames = (from company in session.Query<Company>()
                                              where company.Name.StartsWith("Some")
                                              select new Company { Name = company.Name })
                                                 .ToArray();

                    Assert.Equal(2, companyNames.Length);
                    Assert.Null(companyNames[0].Address1);
                    Assert.Equal("Some Company 1", companyNames[0].Name);
                    Assert.Null(companyNames[1].Address2);
                    Assert.Equal("Some Company 2", companyNames[1].Name);
                }
            }
        }
    }
}
