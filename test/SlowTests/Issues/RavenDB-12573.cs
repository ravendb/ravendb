// -----------------------------------------------------------------------
//  <copyright file="RavenDB_12573.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Orders;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12573 : RavenTestBase
    {
        [Fact]
        public void CanGetDistinctResultWithSkipAndProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        Address = new Address
                        {
                            Country = "Israel"
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Employee>()
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.Country == "UK")
                        .Skip(1)
                        .Distinct()
                        .Select(x => new {
                            Country = x.Address.Country
                        })
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(0, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.Country == "Israel")
                        .Skip(1)
                        .Distinct()
                        .Select(x => new {
                            Country = x.Address.Country
                        })
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.Country == "Israel")
                        .Distinct()
                        .Select(x => new {
                            Country = x.Address.Country
                        })
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(1, stats.TotalResults);
                }
            }
        }

        [Fact]
        public void CanGetDistinctResultWithSkipAndProjectionMultipleDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new Employee
                        {
                            Address = new Address
                            {
                                Country = "Israel"
                            }
                        });
                    }

                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new Employee
                        {
                            Address = new Address
                            {
                                Country = "USA"
                            }
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Employee>()
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.Country == "Israel")
                        .Skip(1)
                        .Take(1)
                        .Select(x => new {
                            Country = x.Address.Country
                        })
                        .Distinct()
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(10, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Address.Country == "Israel")
                        .Skip(0)
                        .Take(1)
                        .Select(x => new {
                            Country = x.Address.Country
                        })
                        .Distinct()
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(10, stats.TotalResults);
                }
            }
        }
    }
}
