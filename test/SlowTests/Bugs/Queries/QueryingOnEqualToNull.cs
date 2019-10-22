//-----------------------------------------------------------------------
// <copyright file="QueryingOnEqualToNull.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOnEqualToNull : RavenTestBase
    {
        public QueryingOnEqualToNull(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryingOnEqNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Company
                    {
                        Phone = 1,
                        Type = Company.CompanyType.Public,
                        Name = null
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    int actual = s.Query<Company>().Where(x => x.Name == null).Count();
                    Assert.Equal(1, actual);
                }
            }
        }

        [Fact]
        public void QueryingOnEqNotNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Company
                    {
                        Phone = 1,
                        Type = Company.CompanyType.Public,
                        Name = null
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    int actual = s.Query<Company>().Where(x => x.Id != "companies/1-A").Count();
                    Assert.Equal(0, actual);
                }
            }
        }
    }
}
