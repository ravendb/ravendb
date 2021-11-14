using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
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

        protected DocumentStore GetLoadedStore(string jsEngineType)
        {
            var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType));
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

        public List<T> Query<T>(string jsEngineType, string q, T type, Dictionary<string, object> parameters = null)
        {
            using (var store = GetLoadedStore(jsEngineType))
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
        [JavaScriptEngineClassData]
        public void QueriesUsingArrowFunc(string jsEngineType)
        {
            var actual = Query(jsEngineType, @"
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
        [InlineData(SimpleFieldProjectionScript1, "Jint")]
        [InlineData(SimpleFieldProjectionScript2, "Jint")]
        [InlineData(SimpleFieldProjectionScript3, "Jint")]
        [InlineData(SimpleFieldProjectionScript4, "Jint")]
        [InlineData(SimpleFieldProjectionScript1, "V8")]
        [InlineData(SimpleFieldProjectionScript2, "V8")]
        [InlineData(SimpleFieldProjectionScript3, "V8")]
        [InlineData(SimpleFieldProjectionScript4, "V8")]
        public void SimpleFieldProjection(string q, string jsEngineType)
        {
            var actual = Query(jsEngineType, q, new { City = "" });
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
        [InlineData(TestProjectionsOfConstAndVariablesScript1, "1", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript2, "1.2", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript3, "1234", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript4, "hello there", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript5, "hello there", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript6, "Categories", "Jint")]
        [InlineData(TestProjectionsOfConstAndVariablesScript1, "1", "V8")]
        [InlineData(TestProjectionsOfConstAndVariablesScript2, "1.2", "V8")]
        [InlineData(TestProjectionsOfConstAndVariablesScript3, "1234", "V8")]
        [InlineData(TestProjectionsOfConstAndVariablesScript4, "hello there", "V8")]
        [InlineData(TestProjectionsOfConstAndVariablesScript5, "hello there", "V8")]
        [InlineData(TestProjectionsOfConstAndVariablesScript6, "Categories", "V8")]
        public void TestProjectionsOfConstAndVariables(string q, string v, string jsEngineType)
        {
            var actual = Query(jsEngineType, q, new { V = "" }, new Dictionary<string, object>
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
        [InlineData(includesScript1, "Jint")]
        [InlineData(includesScript2, "Jint")]
        [InlineData(includesScript3, "Jint")]
        [InlineData(includesScript4, "Jint")]
        [InlineData(includesScript1, "V8")]
        [InlineData(includesScript2, "V8")]
        [InlineData(includesScript3, "V8")]
        [InlineData(includesScript4, "V8")]
        public void Includes(string q, string jsEngineType)
        {
            using (var store = GetLoadedStore(jsEngineType))
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
        [InlineData(FilterByPropertyScript1, "Jint")]
        [InlineData(FilterByPropertyScript2, "Jint")]
        [InlineData(FilterByPropertyScript3, "Jint")]
        [InlineData(FilterByPropertyScript4, "Jint")]
        [InlineData(FilterByPropertyScript1, "V8")]
        [InlineData(FilterByPropertyScript2, "V8")]
        [InlineData(FilterByPropertyScript3, "V8")]
        [InlineData(FilterByPropertyScript4, "V8")]
        public void FilterByProperty(string q, string jsEngineType)
        {
            var actual = Query(jsEngineType, q, new Company { }, new Dictionary<string, object>
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
        [InlineData(projectingRelatedScript1, "Phoebe", "Oscar", "Jint")]
        [InlineData(projectingRelatedScript2, "Oscar", null, "Jint")]
        [InlineData(projectingRelatedScript3, "Oscar Aharon-Eini", "undefined undefined", "Jint")]
        [InlineData(projectingRelatedScript4, "Oscar Aharon-Eini", null, "Jint")]
        [InlineData(projectingRelatedScript5, "Phoebe Eini", "Oscar Aharon-Eini", "Jint")]
        [InlineData(projectingRelatedScript1, "Phoebe", "Oscar", "V8")]
        [InlineData(projectingRelatedScript2, "Oscar", null, "V8")]
        [InlineData(projectingRelatedScript3, "Oscar Aharon-Eini", "undefined undefined", "V8")]
        [InlineData(projectingRelatedScript4, "Oscar Aharon-Eini", null, "V8")]
        [InlineData(projectingRelatedScript5, "Phoebe Eini", "Oscar Aharon-Eini", "V8")]
        public void ProjectingRelated(string q, string nameA, string nameB, string jsEngineType)
        {
            var actual = Query(jsEngineType, q, new { A = "", B = "" });
            Assert.Equal(1, actual.Count);
            Assert.Equal(new { A = nameA, B = nameB }, actual[0]);
        }
    }
}
