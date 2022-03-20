using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16818 : RavenTestBase
    {
        public RavenDB_16818(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_Be_Able_To_Project_Arrays_From_Stored_Index_Fields_Correctly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);

                    var employee = new Employee { FirstName = "John", LastName = "Doe" };
                    session.Store(employee);

                    for (var i = 0; i < 2; i++)
                    {
                        var order = new Order
                        {
                            Company = company.Id,
                            Employee = employee.Id
                        };

                        session.Store(order);
                    }

                    session.SaveChanges();
                }

                new Orders_ByEmployee().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<ProjectionResult>(@"
from index 'Orders/ByEmployee' as i
load i.Orders as o[]
select {
    OrdersBug: o,
    Orders: load(i.Orders)
}").ToList();

                    Assert.Equal(1, results.Count);

                    foreach (var result in results)
                    {
                        Assert.Equal(2, result.Orders.Length);
                        Assert.Equal(result.Orders.Length, result.OrdersBug.Length);
                    }
                }
            }
        }

        private class ProjectionResult
        {
            public Order[] OrdersBug { get; set; }

            public Order[] Orders { get; set; }
        }

        private class Orders_ByEmployee : AbstractIndexCreationTask<Order, Orders_ByEmployee.Entry>
        {
            public class Entry
            {
                public string Employee { get; set; }

                public List<string> Orders { get; set; }
            }

            public Orders_ByEmployee()
            {
                Map = orders => from order in orders
                                let employee = LoadDocument<Employee>(order.Employee)
                                let company = LoadDocument<Company>(order.Company)
                                select new Entry
                                {
                                    Employee = employee.Id,
                                    Orders = new List<string> { order.Id }
                                };

                Reduce = results => from result in results
                                    group result by new
                                    {
                                        result.Employee
                                    }
                    into g
                                    select new Entry
                                    {
                                        Employee = g.Key.Employee,
                                        Orders = g.SelectMany(x => x.Orders).ToList()
                                    };

                Stores.Add(x => x.Orders, FieldStorage.Yes);
            }
        }
    }
}
