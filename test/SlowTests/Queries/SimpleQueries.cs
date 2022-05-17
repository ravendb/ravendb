using Tests.Infrastructure;
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

        protected DocumentStore GetLoadedStore(Options options)
        {
            var store = GetDocumentStore(options);
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

        public List<T> Query<T>(Options options, string q, T type, Dictionary<string, object> parameters = null)
        {
            using (var store = GetLoadedStore(options))
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
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void QueriesUsingArrowFunc(Options options)
        {
            var actual = Query(options, @"
from Employees as e
select {
    Notes: e.Notes.map(x=>x.toUpperCase())
}
", new Employee());

            Assert.Equal(2, actual.Count);
            Assert.Equal(new[] { "DARK", "DOG", "SMALL" }, actual[0].Notes);
            Assert.Equal(new[] { "PALE", "DOG", "BIG" }, actual[1].Notes);

        }


        const string SimpleFieldProjectionScript1 = "from Companies select Address.City as City";
        const string SimpleFieldProjectionScript2 = "from Companies c select c.Address.City as City";
        const string SimpleFieldProjectionScript3 = "from Companies as c select c.Address.City as City";
        const string SimpleFieldProjectionScript4 = "from Companies as c select { City: c.Address.City }";

        [Theory]
        [RavenData(SimpleFieldProjectionScript1, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(SimpleFieldProjectionScript2, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(SimpleFieldProjectionScript3, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(SimpleFieldProjectionScript4, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void SimpleFieldProjection(Options options, string q)
        {
            var actual = Query(options, q, new { City = "" });
            Assert.Equal(new[]
            {
                new {City = "Hadera" },
                new {City = "Toruń" },
                new {City = "Buenos Aires" },
            },
                actual);
        }

        const string TestProjectionsOfConstAndVariablesScript1 = "from Categories select 1 as V";
        const string TestProjectionsOfConstAndVariablesScript2 = "from Categories select 1.2 as V";
        const string TestProjectionsOfConstAndVariablesScript3 = "from Categories select $t as V";
        const string TestProjectionsOfConstAndVariablesScript4 = "from Categories select 'hello there' as V";
        const string TestProjectionsOfConstAndVariablesScript5 = "from Categories select \"hello there\" as V";
        const string TestProjectionsOfConstAndVariablesScript6 = "from Categories as c select {V: c['@metadata']['@collection'] }";

        [Theory]
        [RavenData(TestProjectionsOfConstAndVariablesScript1, "1", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(TestProjectionsOfConstAndVariablesScript2, "1.2", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(TestProjectionsOfConstAndVariablesScript3, "1234", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(TestProjectionsOfConstAndVariablesScript4, "hello there", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(TestProjectionsOfConstAndVariablesScript5, "hello there", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(TestProjectionsOfConstAndVariablesScript6, "Categories", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void TestProjectionsOfConstAndVariables(Options options, string q, string v)
        {
            var actual = Query(options, q, new { V = "" }, new Dictionary<string, object>
            {
                ["t"] = 1234
            });
            Assert.Equal(new[]
                {
                    new {V = v },
                },
                actual);

        }

        private const string includesScript1 = "from Employees where FirstName = 'Phoebe' include ReportsTo";
        private const string includesScript2 = "from Employees as e where e.FirstName = 'Phoebe' include e.ReportsTo";
        private const string includesScript3 = "from Employees where FirstName = 'Oscar' include ReportsTo";

        private const string includesScript4 = @"
declare function project(e){
    include(e.ReportsTo)
    return e;
}
from Employees as e 
where e.FirstName = 'Oscar'
select project(e)";

        [Theory]
        [RavenData(includesScript1, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(includesScript2, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(includesScript3, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(includesScript4, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Includes(Options options, string q)
        {
            using (var store = GetLoadedStore(options))
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


        const string FilterByPropertyScript1 = "from Companies where Address.City = 'Hadera'";
        const string FilterByPropertyScript2 = "from Companies c where c.Address.City = 'Hadera'";
        const string FilterByPropertyScript3 = "from Companies c where c.Address.City != 'Toruń' and c.Address.City != 'Buenos Aires'";
        const string FilterByPropertyScript4 = "from Companies where Address.City = $city ";

        [Theory]
        [RavenData(FilterByPropertyScript1, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(FilterByPropertyScript2, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(FilterByPropertyScript3, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(FilterByPropertyScript4, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void FilterByProperty(Options options, string q)
        {
            var actual = Query(options, q, new Company { }, new Dictionary<string, object>
            {
                ["city"] = "Hadera"
            });
            Assert.Equal(1, actual.Count);
            Assert.Equal("Hadera", actual[0].Address.City);
        }

        const string projectingRelatedScript1 = @"
from Employees e 
where e.FirstName = 'Phoebe' 
load e.ReportsTo r 
select e.FirstName as A, r.FirstName as B";

        const string projectingRelatedScript2 = @"
from Employees e 
where e.FirstName = 'Oscar' 
load e.ReportsTo r 
select e.FirstName as A, r.FirstName as B";

        const string projectingRelatedScript3 = @"
from Employees e 
where e.FirstName = 'Oscar' 
load e.ReportsTo r 
select {     
    A: e.FirstName +' '+e.LastName, 
    B: r?.FirstName +' ' + r?.LastName 
} ";

        const string projectingRelatedScript4 = @"
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
} ";

        const string projectingRelatedScript5 = @"
from Employees e 
where e.FirstName = 'Phoebe' 
load e.ReportsTo r 
select { 
    A: e.FirstName +' '+e.LastName, 
    B: r.FirstName +' ' + r.LastName 
}";

        [Theory]
        [RavenData(projectingRelatedScript1, "Phoebe", "Oscar", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(projectingRelatedScript2, "Oscar", null, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(projectingRelatedScript3, "Oscar Aharon-Eini", "undefined undefined", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(projectingRelatedScript4, "Oscar Aharon-Eini", null, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(projectingRelatedScript5, "Phoebe Eini", "Oscar Aharon-Eini", JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ProjectingRelated(Options options, string q, string nameA, string nameB)
        {
            var actual = Query(options, q, new { A = "", B = "" });
            Assert.Equal(1, actual.Count);
            Assert.Equal(new { A = nameA, B = nameB }, actual[0]);
        }
    }
}
