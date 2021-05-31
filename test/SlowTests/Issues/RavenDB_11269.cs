using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11269 : RavenTestBase
    {
        public RavenDB_11269(ITestOutputHelper output) : base(output)
        {
        }

        private class Employee
        {
            public string FirstName { get; set; }
            public Address Address { get; set; }
        }

        [Fact]
        public void CanLoadViaLetWithSimpleNonJsProjection()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");

                    session.Store(new Order
                    {
                        Company   = "HR",
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
	                            let employee = RavenQuery.Load<Employee>(o.Employee)
	                            select employee;

                    Assert.Equal("from 'Orders' as o load o.Employee as employee select employee", query.ToString());

                    List<Employee> employees = query.ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Equal("Jerry", employees[0].FirstName);
                }
                
            }
        }

        [Fact]
        public void CanLoadViaLetAndProjectMember()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");

                    session.Store(new Order
                    {
                        Company = "HR",
                        Employee = "employees/1-A"
                    }, "orders/1-A");

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let employee = RavenQuery.Load<Employee>(o.Employee)
                                select employee.FirstName;

                    Assert.Equal("from 'Orders' as o load o.Employee as employee select employee.FirstName", query.ToString());

                    var employees = query.ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Equal("Jerry", employees[0]);
                }

            }
        }

        [Fact]
        public void CanDoMultipuleLoadsViaLetAndProjectMember()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");

                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        Employee = "employees/1-A"
                    }, "orders/1-A");

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let employee = RavenQuery.Load<Employee>(o.Employee)
                                let company = RavenQuery.Load<Employee>(o.Company)
                                select employee.FirstName;

                    Assert.Equal("from 'Orders' as o load o.Employee as employee, o.Company as company select employee.FirstName", query.ToString());

                    var employees = query.ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Equal("Jerry", employees[0]);
                }

            }
        }

        [Fact]
        public void CanLoadViaLetAndProjectNestedMember()
        {
            using (var store = GetDocumentStore())
            {           
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address
                        {
                            City = "Berlin",
                            Country = "Germany"
                        }
                    }, "employees/1-A");

                    session.Store(new Order
                    {
                        Company = "HR",
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let employee = RavenQuery.Load<Employee>(o.Employee)
                                select employee.Address.Country;

                    Assert.Equal("from 'Orders' as o load o.Employee as employee select employee.Address.Country", query.ToString());
                    var employeesCountry = query.ToList();

                    Assert.Equal(1, employeesCountry.Count);
                    Assert.Equal("Germany", employeesCountry[0]);
                }

            }
        }

        [Fact]
        public void CanProjectMemberWithFromAlias()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee(), "employees/1-A");

                    session.Store(new Order
                    {
                        Employee = "employees/1-A",
                        ShipTo = new Address
                        {
                            City = "Berlin",
                            Country = "Germany"
                        }
                    }, "orders/1-A");
                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let employee = RavenQuery.Load<Employee>(o.Employee)
                                select o.ShipTo.Country;

                    Assert.Equal("from 'Orders' as o load o.Employee as employee select o.ShipTo.Country", query.ToString());
                    var shipToCountries = query.ToList();

                    Assert.Equal(1, shipToCountries.Count);
                    Assert.Equal("Germany", shipToCountries[0]);
                }

            }
        }

    }
}
