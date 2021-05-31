using System.Diagnostics.CodeAnalysis;
using System.Linq;
using FastTests.Server.Basic.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Dynamic.MapReduce
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class GroupByMultipleFields : RavenTestBase
    {
        public GroupByMultipleFields(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Group_by_multiple_fields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1",
                        Company = "companies/2"
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/1",
                        Company = "companies/2"
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/2",
                        Company = "companies/2"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new
                            {
                                x.Employee,
                                x.Company
                            })
                            .Select(x => new
                            {
                                x.Key.Employee,
                                x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].Employee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].Employee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new GroupByEmployeeAndCompany // class instead of anonymous object
                            {
                                Employee = x.Employee,
                                Company = x.Company
                            })
                            .Select(x => new
                            {
                                x.Key.Employee,
                                x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].Employee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].Employee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new
                            {
                                RenamedEmployee = x.Employee, // field rename inside GroupBy
                                x.Company
                            })
                            .Select(x => new
                            {
                                x.Key.RenamedEmployee,
                                x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].RenamedEmployee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].RenamedEmployee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new GroupByRenamedEmployeeAndCompany // class instead of anonymous object
                            {

                                RenamedEmployee = x.Employee, // field renames inside GroupBy
                                Company = x.Company
                            })
                            .Select(x => new
                            {
                                x.Key.RenamedEmployee,
                                x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].RenamedEmployee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].RenamedEmployee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new
                            {
                                x.Employee,
                                x.Company
                            })
                            .Select(x => new
                            {
                                RenamedEmployee = x.Key.Employee, // field rename of composite key inside Select
                                x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].RenamedEmployee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].RenamedEmployee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new
                            {
                                x.Employee,
                                x.Company
                            })
                            .Select(x => new GroupByRenamedEmployeeAndCompanyResult // class instead of anonymous object
                            {
                                RenamedEmployee = x.Key.Employee, // field rename of composite key inside Select
                                Company = x.Key.Company,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].RenamedEmployee);
                    Assert.Equal("companies/2", orders[0].Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].RenamedEmployee);
                    Assert.Equal("companies/2", orders[1].Company);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => new
                            {
                                x.Employee,
                                RenamedCompany = x.Company // renamed here
                            })
                            .Select(x => new
                            {
                                RenamedEmployee = x.Key.Employee, // and here
                                x.Key.RenamedCompany,
                                Count = x.Count()
                            })
                            .OrderBy(x => x.Count)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].RenamedEmployee);
                    Assert.Equal("companies/2", orders[0].RenamedCompany);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].RenamedEmployee);
                    Assert.Equal("companies/2", orders[1].RenamedCompany);
                }
            }
        }

        [Fact]
        public void Select_composite_group_by_key()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1",
                        Company = "companies/2"
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/1",
                        Company = "companies/2"
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/2",
                        Company = "companies/2"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => new
                        {
                            x.Employee,
                            x.Company
                        })
                        .Select(x => new
                        {
                            x.Key,
                            Count = x.Count()
                        })
                        .OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal(1, orders[0].Count);
                    Assert.Equal("employees/2", orders[0].Key.Employee);
                    Assert.Equal("companies/2", orders[0].Key.Company);

                    Assert.Equal(2, orders[1].Count);
                    Assert.Equal("employees/1", orders[1].Key.Employee);
                    Assert.Equal("companies/2", orders[1].Key.Company);


                    var orders2 = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => new GroupByEmployeeAndCompany
                        {
                            Employee = x.Employee,
                            Company = x.Company
                        })
                        .Select(x => new OrderByCompositeKeyReduceResult
                        {
                            GroupByEmployeeAndCompany = x.Key, // not allowed
                            Count = x.Count()
                        })
                        .OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, orders2.Count);

                    Assert.Equal(1, orders2[0].Count);
                    Assert.Equal("employees/2", orders2[0].GroupByEmployeeAndCompany.Employee);
                    Assert.Equal("companies/2", orders2[0].GroupByEmployeeAndCompany.Company);

                    Assert.Equal(2, orders2[1].Count);
                    Assert.Equal("employees/1", orders2[1].GroupByEmployeeAndCompany.Employee);
                    Assert.Equal("companies/2", orders2[1].GroupByEmployeeAndCompany.Company);
                }
            }
        }

        public class GroupByEmployeeAndCompany
        {
            public string Employee { get; set; }
            public string Company { get; set; }
        }

        public class OrderByCompositeKeyReduceResult
        {
            public GroupByEmployeeAndCompany GroupByEmployeeAndCompany { get; set; }
            public int Count { get; set; }
        }

        public class GroupByRenamedEmployeeAndCompany
        {
            public string RenamedEmployee { get; set; }
            public string Company { get; set; }
        }

        public class GroupByRenamedEmployeeAndCompanyResult
        {
            public string RenamedEmployee { get; set; }
            public string Company { get; set; }
            public int Count { get; set; }
        }
    }
}
