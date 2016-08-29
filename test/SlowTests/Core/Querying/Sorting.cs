// -----------------------------------------------------------------------
//  <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;

using FastTests;

using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Querying
{
    public class Sorting : RavenTestBase
    {
        [Fact]
        public void BasicSorting()
        {
            using (var store = GetDocumentStore())
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
