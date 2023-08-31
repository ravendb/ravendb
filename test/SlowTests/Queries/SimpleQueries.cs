using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Queries
{
    public class SimpleQueries : RavenTestBase
    {
        public SimpleQueries(ITestOutputHelper output) : base(output)
        {
        }

        protected DocumentStore GetLoadedStore()
        {
            var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new Category());
                var oscar = new Employee
                {
                    FirstName = "Oscar",
                    LastName = "Aharon-Eini",
                    Notes = new List<string>
                    {
                        "Dark",
                        "Dog",
                        "Small"
                    }
                };
                session.Store(oscar);
                session.Store(new Employee
                {
                    FirstName = "Phoebe",
                    LastName = "Eini",
                    ReportsTo = oscar.Id,
                    Notes = new List<string>
                    {
                        "Pale",
                        "Dog",
                        "Big"
                    }
                });

                session.Store(new Company
                {
                    Name = "One",
                    Address = new Address
                    {
                        City = "Hadera"
                    }
                });
                session.Store(new Company
                {
                    Name = "Two",
                    Address = new Address
                    {
                        City = "Toruń"
                    }
                });
                session.Store(new Company
                {
                    Name = "Three",
                    Address = new Address
                    {
                        City = "Buenos Aires"
                    }
                });
                session.SaveChanges();
            }

            return store;
        }

        public List<T> Query<T>(string q, T type, Dictionary<string, object> parameters = null)
        {
            using (var store = GetLoadedStore())
            {
                //WaitForUserToContinueTheTest(store);
                using (var s = store.OpenSession())
                {
                    var documentQuery = s.Advanced.RawQuery<T>(q);
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            documentQuery.AddParameter(parameter.Key, parameter.Value);
                        }
                    }
                    return documentQuery.WaitForNonStaleResults()
                        .ToList();
                }
            }
        }
        [Fact]
        public void QueriesUsingArrowFunc()
        {
            var actual = Query(@"
from Employees as e
select {
    Notes: e.Notes.map(x=>x.toUpperCase())
}
", new Employee());

            Assert.Equal(2, actual.Count);
            Assert.Equal(new[] { "DARK", "DOG", "SMALL" }, actual[0].Notes);
            Assert.Equal(new[] { "PALE", "DOG", "BIG" }, actual[1].Notes);

        }
        [Theory]
        [InlineData("from Companies select Address.City as City")]
        [InlineData("from Companies c select c.Address.City as City")]
        [InlineData("from Companies as c select c.Address.City as City")]
        [InlineData("from Companies as c select { City: c.Address.City }")]
        public void SimpleFieldProjection(string q)
        {
            var actual = Query(q, new { City = "" });
            Assert.Equal(new[]
            {
                new {City = "Hadera" },
                new {City = "Toruń" },
                new {City = "Buenos Aires" },
            },
                actual);
        }


        [Theory]
        [InlineData("from Categories select 1 as V", "1")]
        [InlineData("from Categories select 1.2 as V", "1.2")]
        [InlineData("from Categories select $t as V", "1234")]
        [InlineData("from Categories select 'hello there' as V", "hello there")]
        [InlineData("from Categories select \"hello there\" as V", "hello there")]
        [InlineData("from Categories as c select {V: c['@metadata']['@collection'] }", "Categories")]
        public void TestProjectionsOfConstAndVariables(string q, string v)
        {
            var actual = Query(q, new { V = "" }, new Dictionary<string, object>
            {
                ["t"] = 1234
            });
            Assert.Equal(new[]
                {
                    new {V = v },
                },
                actual);

        }

        [Theory]
        [InlineData("from Employees where FirstName = 'Phoebe' include ReportsTo")]
        [InlineData("from Employees as e where e.FirstName = 'Phoebe' include e.ReportsTo")]
        [InlineData("from Employees where FirstName = 'Oscar' include ReportsTo")]
        [InlineData(@"
declare function project(e){
    include(e.ReportsTo)
    return e;
}
from Employees as e 
where e.FirstName = 'Oscar'
select project(e)")]
        public void Includes(string q)
        {
            using (var store = GetLoadedStore())
            {
                using (var s = store.OpenSession())
                {
                    var employees = s.Advanced
                        .RawQuery<Employee>(q)
                        .ToList();

                    Assert.Equal(1, employees.Count);


                    Assert.Equal(1, s.Advanced.NumberOfRequests);

                    if (employees[0].ReportsTo != null)
                        s.Load<Employee>(employees[0].ReportsTo);

                    Assert.Equal(1, s.Advanced.NumberOfRequests);
                }
            }

        }

        [Theory]
        [InlineData("from Companies where Address.City = 'Hadera'")]
        [InlineData("from Companies c where c.Address.City = 'Hadera'")]
        [InlineData("from Companies c where c.Address.City != 'Toruń' and c.Address.City != 'Buenos Aires'")]
        [InlineData("from Companies where Address.City = $city ")]
        public void FilterByProperty(string q)
        {
            var actual = Query(q, new Company { }, new Dictionary<string, object>
            {
                ["city"] = "Hadera"
            });
            Assert.Equal(1, actual.Count);
            Assert.Equal("Hadera", actual[0].Address.City);
        }

        [Theory]
        [InlineData(@"
from Employees e 
where e.FirstName = 'Phoebe' 
load e.ReportsTo r 
select e.FirstName as A, r.FirstName as B", "Phoebe", "Oscar")]
        [InlineData(@"
from Employees e 
where e.FirstName = 'Oscar' 
load e.ReportsTo r 
select e.FirstName as A, r.FirstName as B", "Oscar", null)]
        [InlineData(@"
from Employees e 
where e.FirstName = 'Oscar' 
load e.ReportsTo r 
select {     
    A: e.FirstName +' '+e.LastName, 
    B: r.FirstName +' ' + r.LastName 
} ", "Oscar Aharon-Eini", "null null")]
        [InlineData(@"
declare function name(e) {
    if( e == null)
        return null;
    return e.FirstName +' ' + e.LastName; 
}
from Employees e 
where e.FirstName = 'Oscar' 
load e.ReportsTo r 
select { 
    A: name(e), 
    B: name(r)
} ", "Oscar Aharon-Eini", null)]
        [InlineData(@"
from Employees e 
where e.FirstName = 'Phoebe' 
load e.ReportsTo r 
select { 
    A: e.FirstName +' '+e.LastName, 
    B: r.FirstName +' ' + r.LastName 
} ", "Phoebe Eini", "Oscar Aharon-Eini")]
        public void ProjectingRelated(string q, string nameA, string nameB)
        {
            var actual = Query(q, new { A = "", B = "" });
            Assert.Equal(1, actual.Count);
            Assert.Equal(new { A = nameA, B = nameB }, actual[0]);
        }
    }
}
