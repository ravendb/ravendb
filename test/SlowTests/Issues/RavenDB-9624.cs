using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class QueriesWithReservedWords : RavenTestBase
    {
        public QueriesWithReservedWords(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Use_From_Alias_thats_a_Reserved_Word_in_RQL()
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
                        Employee = "employees/1-A",
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from order in session.Query<Order>()
                                select new
                                {
                                    Employee = session.Load<Employee>(order.Employee),
                                    Company = order.Company,
                                };

                    Assert.Equal("from 'Orders' as 'order' select { Employee : load(order.Employee), Company : order.Company }"
                        , query.ToString());

                    var result = query.ToList();

                    Assert.Equal("Jerry", result[0].Employee.FirstName);
                    Assert.Equal("companies/1-A", result[0].Company);

                }
            }
        }

        [Fact]
        public void Can_Use_From_Alias_thats_a_Reserved_Word_in_Javascript()
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
                        Employee = "employees/1-A",
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from function in session.Query<Order>()
                                select new
                                {
                                    Employee = session.Load<Employee>(function.Employee),
                                    Company = function.Company,
                                };

                    Assert.Equal("from 'Orders' as _function select { " +
                                 "Employee : load(_function.Employee), Company : _function.Company }"
                                , query.ToString());

                    var result = query.ToList();

                    Assert.Equal("Jerry", result[0].Employee.FirstName);
                    Assert.Equal("companies/1-A", result[0].Company);

                }
            }
        }

        [Fact]
        public void Can_Use_From_Alias_thats_a_Reserved_Word_in_Both_RQL_and_Javascript()
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
                        Employee = "employees/1-A",
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    //load is reserved both in rql and js

                    var query = from load in session.Query<Order>()
                                select new
                                {
                                    Employee = session.Load<Employee>(load.Employee),
                                    Company = load.Company,
                                };

                    Assert.Equal("from 'Orders' as _load select { Employee : load(_load.Employee), Company : _load.Company }"
                        , query.ToString());

                    var result = query.ToList();

                    Assert.Equal("Jerry", result[0].Employee.FirstName);
                    Assert.Equal("companies/1-A", result[0].Company);

                }
            }
        }

        [Fact]
        public void Can_Use_Let_with_From_Alias_thats_a_Reserved_Word_in_RQL()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "GD",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 10,
                                Quantity = 5
                            },
                            new OrderLine
                            {
                                PricePerUnit = 20,
                                Quantity = 10
                            }
                        }
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from order in session.Query<Order>()
                                where order.Company == "GD"
                                let sum = order.Lines.Sum(l => l.PricePerUnit * l.Quantity)
                                select new
                                {
                                    Sum = sum
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(__alias0) {
	var order = __alias0;
	var sum = order.Lines.map(function(l){return l.PricePerUnit*l.Quantity;}).reduce(function(a, b) { return a + b; }, 0);
	return { Sum : sum };
}
from 'Orders' as __alias0 where __alias0.Company = $p0 select output(__alias0)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(250, result[0].Sum);

                }
            }
        }

        [Fact]
        public void Can_Use_Let_with_Multipule_Aliases_that_are_Reserved_Words_in_RQL()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/2-A");
                    session.Store(new Company
                    {
                        Name = "GD",
                        AccountsReceivable = 2.0m,
                        EmployeesIds = new List<string>
                        {
                            "employees/1-A" , "employees/2-A"
                        }
                    }, "companies/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 10,
                                Quantity = 5
                            },
                            new OrderLine
                            {
                                PricePerUnit = 20,
                                Quantity = 10
                            }
                        }
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    //the from-alias is also a reserved word

                    var query = from order in session.Query<Order>()
                                let include = order.Company
                                let load = session.Load<Company>(include)
                                let update = RavenQuery.Load<Employee>(load.EmployeesIds)
                                let sum = order.Lines.Sum(l => l.PricePerUnit * l.Quantity * load.AccountsReceivable)
                                select new
                                {
                                    Comapny = load,
                                    Sum = sum,
                                    Employees = update.Select(e => e.FirstName).ToList()
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(__alias0) {
	var order = __alias0;
	var include = order.Company;
	var _load = load(include);
	var update = load(_load.EmployeesIds);
	var sum = order.Lines.map(function(l){return l.PricePerUnit*l.Quantity*_load.AccountsReceivable;}).reduce(function(a, b) { return a + b; }, 0);
	return { Comapny : _load, Sum : sum, Employees : update.map(function(e){return e.FirstName;}) };
}
from 'Orders' as __alias0 select output(__alias0)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal("GD", result[0].Comapny.Name);
                    Assert.Equal(500, result[0].Sum);
                    Assert.Equal(2, result[0].Employees.Count);
                    Assert.Equal("Jerry", result[0].Employees[0]);
                    Assert.Equal("Bob", result[0].Employees[1]);

                }
            }
        }

        [Fact]
        public void Can_Load_via_Let_with_Alias_Thats_a_Reserved_Word_in_RQL()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "GD"
                    }, "companies/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let update = session.Load<Company>(o.Company)
                                select new
                                {
                                    Company = update.Name,
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, __alias0) {
	var update = __alias0;
	return { Company : update.Name };
}
from 'Orders' as o load o.Company as __alias0 select output(o, __alias0)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal("GD", result[0].Company);
                }
            }
        }

        [Fact]
        public void Can_Load_with_Multipule_Aliases_That_are_Reserved_Words_in_RQL()
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
                        Name = "GD"
                    }, "companies/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        Employee = "employees/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    //the from-alias is not a reserved word

                    var query = from o in session.Query<Order>()
                                let update = session.Load<Company>(o.Company)
                                let include = session.Load<Employee>(o.Employee)
                                select new
                                {
                                    Company = update.Name,
                                    Employee = include.FirstName
                                };
                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, __alias0) {
	var update = __alias0;
	var include = load(o.Employee);
	return { Company : update.Name, Employee : include.FirstName };
}
from 'Orders' as o load o.Company as __alias0 select output(o, __alias0)"
                , query.ToString());

                    var result = query.ToList();

                    Assert.Equal("GD", result[0].Company);
                    Assert.Equal("Jerry", result[0].Employee);
                }
            }
        }

        [Fact]
        public void Can_do_Multipule_Loads_Where_1st_has_Alias_thats_a_Reserved_Word_and_2nd_has_LoadArg_thats_dependent_on_the_1st_Alias()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/2-A");

                    session.Store(new Company
                    {
                        Name = "GD",
                        EmployeesIds = new List<string>
                        {
                            "employees/1-A",
                            "employees/2-A"
                        }
                    }, "companies/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let update = session.Load<Company>(o.Company)
                                let employees = RavenQuery.Load<Employee>(update.EmployeesIds)
                                select new
                                {
                                    Company = update.Name,
                                    Employees = employees.Select(e => e.FirstName).ToList()
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, __alias0) {
	var update = __alias0;
	var employees = load(update.EmployeesIds);
	return { Company : update.Name, Employees : employees.map(function(e){return e.FirstName;}) };
}
from 'Orders' as o load o.Company as __alias0 select output(o, __alias0)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal("GD", result[0].Company);

                    Assert.Equal(2, result[0].Employees.Count);
                    Assert.Equal("Jerry", result[0].Employees[0]);
                    Assert.Equal("Bob", result[0].Employees[1]);
                }
            }
        }

        [Fact]
        public void Can_Use_Let_with_Variable_Name_thats_a_Reserved_Word_in_Javascript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "GD",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 10,
                                Quantity = 5
                            },
                            new OrderLine
                            {
                                PricePerUnit = 20,
                                Quantity = 10
                            }
                        }
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let function = o.Lines.Sum(l => l.PricePerUnit * l.Quantity)
                                select new
                                {
                                    Sum = function
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o) {
	var _function = o.Lines.map(function(l){return l.PricePerUnit*l.Quantity;}).reduce(function(a, b) { return a + b; }, 0);
	return { Sum : _function };
}
from 'Orders' as o select output(o)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(250, result[0].Sum);
                }
            }
        }

        [Fact]
        public void Can_Load_with_Multipule_Aliases_That_are_Reserved_Words_in_Javascript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/2-A");
                    session.Store(new Company
                    {
                        AccountsReceivable = 2.0m,
                        Name = "GD",
                        EmployeesIds = new List<string>
                        {
                            "employees/1-A" , "employees/2-A"
                        }
                    }, "companies/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let function = session.Load<Company>(o.Company)
                                let super = function.AccountsReceivable
                                let var = RavenQuery.Load<Employee>(function.EmployeesIds)
                                select new
                                {
                                    Company = function,
                                    Number = super,
                                    Employees = var.Select(e => e.FirstName).ToList()
                                };
                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, _function) {
	var _super = _function.AccountsReceivable;
	var _var = load(_function.EmployeesIds);
	return { Company : _function, Number : _super, Employees : _var.map(function(e){return e.FirstName;}) };
}
from 'Orders' as o load o.Company as _function select output(o, _function)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal("GD", result[0].Company.Name);
                    Assert.Equal(2, result[0].Number);
                    Assert.Equal(2, result[0].Employees.Count);
                    Assert.Equal("Jerry", result[0].Employees[0]);
                    Assert.Equal("Bob", result[0].Employees[1]);

                }
            }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Projections_Names()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        Employee = "employees/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                select new
                                {
                                    Load = o.Company,
                                    Include = o.Employee
                                };

                    var result = query.ToList();

                    Assert.Equal("from 'Orders' select Company as 'Load', Employee as 'Include'", query.ToString());

                    Assert.Equal("companies/1-A", result[0].Load);
                    Assert.Equal("employees/1-A", result[0].Include);
                }
            }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Projections_Names_Complex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        Employee = "employees/1-A"
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                select new
                                {
                                    Update = o.Company.Substring(10),
                                    Include = o.Employee.Substring(10)
                                };

                    var result = query.ToList();

                    Assert.Equal("from 'Orders' as o select " +
                                 "{ Update : o.Company.substr(10), Include : o.Employee.substr(10) }", query.ToString());

                    Assert.Equal("1-A", result[0].Update);
                    Assert.Equal("1-A", result[0].Include);
                }
            }
        }

    }
}
