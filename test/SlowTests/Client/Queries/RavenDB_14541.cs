using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Client.Queries
{
    public class RavenDB_14541 : RavenTestBase
    {
        public RavenDB_14541(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithMemberInitSplitTest()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(a => new Foo()
                        {
                            _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]),
                            Name = a.FirstName,
                        });
                    var res = query1.ToList();

                    AssertIncludedDocsAndRql(
                        session, 
                        ["Companies/App", "Companies/Raven"], 
                        query1.ToString(), 
                        "declare function output(a) {\r\n\tinclude(a.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Name : a.FirstName };\r\n}\r\nfrom index 'Employees/ByFirstName' as a select output(a)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithSplitAndMathTest()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(a => new
                        {
                            Name = a.FirstName,
                            _ = RavenQuery.Include<Employee>(e => "Companies/dd"+Math.Round(e.Number)),
                            __ = RavenQuery.Include<Employee>(e => e.Company.Split('#', StringSplitOptions.None)[0])
                        });
                    
                    var results = query1.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/dd3", "Companies/dd1", "Companies/Raven", "Companies/App"],
                        query1.ToString(),
                        "declare function output(a) {\r\n\tinclude(\"Companies/dd\"+Math.round(a.Number));\r\n\tinclude(a.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Name : a.FirstName };\r\n}\r\nfrom index 'Employees/ByFirstName' as a select output(a)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithLetSingleSplitTest()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query1 = from doc in session.Query<Employee, Employees_ByFirstName>()
                        let name = doc.FirstName
                        select new
                        {
                            FirstName = name,
                            _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]),
                        };

                    var results = query1.ToList();
                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/App", "Companies/Raven"], 
                        query1.ToString(), 
                        "declare function output(doc) {\r\n\tvar name = doc.FirstName;\r\n\tinclude(doc.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { FirstName : name };\r\n}\r\nfrom index 'Employees/ByFirstName' as doc select output(doc)"
                    );
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void IncludeWithLet()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query2 = from e in session.Query<Employee>()
                        let _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0])
                        select new { FirstName = e.FirstName };

                    var results = query2.ToList();
                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/App", "Companies/Raven"],
                        query2.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { FirstName : e.FirstName };\r\n}\r\nfrom 'Employees' as e select output(e)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void IncludeWithSinglePropertyAndLet()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query3 = from e in session.Query<Employee>()
                        let _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0])
                        select e.FirstName;
                    var results3 = query3.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/Raven", "Companies/App"],
                        query3.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn {FirstName:e.FirstName};\r\n}\r\nfrom 'Employees' as e select output(e)"
                        );
                }
            }
        }


        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void MultipleIncludesInsideSelect()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query4 = session.Query<Employee, Employees_ByFirstName>()
                        .Select
                        (e => new
                        {
                            FirstName = e.FirstName,
                            _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]),
                            __ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[1])
                        });

                    var res = query4.ToList();
                    AssertIncludedDocsAndRql(
                        session, 
                        ["Companies/Amaz", "Companies/App", "Companies/Raven", "Companies/App"], 
                        query4.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[1]);\r\n\treturn { FirstName : e.FirstName };\r\n}\r\nfrom index 'Employees/ByFirstName' as e select output(e)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithInvalidName()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query5 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(e => new
                        {
                            FirstName = e.FirstName,
                            _ = Raven.Client.Documents.Queries.RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]),
                            Include = Raven.Client.Documents.Queries.RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[1])
                        });
                        
                    var error = Assert.Throws<InvalidOperationException>(() => query5.ToList());
                    Assert.Equal("The include variable can only be assigned to the discard character (_)", error.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void ShouldThrowInvalidOperationExceptionHaveIncludesInsteadOf_()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                InitializeData2(store);

                var error = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var query3 = from o in session.Query<Order>()
                                     let includes = RavenQuery.Include<Order>(u => u.Employee)
                                     select new QueryResult { Comapny = o.Company };

                        var results = query3.ToList();
                    }
                });
                Assert.Equal("The include variable can only be assigned to the discard character (_)", error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void SessionQuerySelectAddressFromIncludeDoc_UsingRavenQueryStringWithStateObject()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query3 = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.StateId)
                        select new { Name = a.City };

                    query3.ToList();
                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query3.ToString(),
                        "declare function output(a) {\r\n\tinclude(a.StateId);\r\n\treturn { Name : a.City };\r\n}\r\nfrom 'Addresses' as a select output(a)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void SessionQuerySelectAddressFromIncludeDoc_UsingRavenQueryWithComplexLambdaExpression()
        { 
            using (DocumentStore store = GetDocumentStore())
            {
              InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var address = session.Include<Address>(x => x.CountryState.Split('#', StringSplitOptions.None)[0]).ToString();
                }

                using (var session = store.OpenSession())
                {
                    var query3 = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.CountryState.Split('#', StringSplitOptions.None)[0])
                        select new { Name = a.City };

                    var res2 = query3.ToList();
                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query3.ToString(),
                        "declare function output(a) {\r\n\tinclude(a.CountryState.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Name : a.City };\r\n}\r\nfrom 'Addresses' as a select output(a)");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void SessionQuerySelectAdressFromIncludeDoc_UsingRavenQueryWithSimpleLambdaExpression()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData2(store);
                using (var session = store.OpenSession())
                {
                    var query3 = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.StateId)
                        select new { Name = a.City };
                    var res2 = query3.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query3.ToString(),
                        "declare function output(a) {\r\n\tinclude(a.StateId);\r\n\treturn { Name : a.City };\r\n}\r\nfrom 'Addresses' as a select output(a)");
                }
            }
        }
            

        private void InitializeData1(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var employee1 = new Employee { FirstName = "Golan", Number = 1.2, Company = "Companies/Raven#Companies/Micro" };
                var employee2 = new Employee { FirstName = "Grisha", Number = 2.6, Company = "Companies/App#Companies/Amaz" };
                var company1 = new Company { Name = "RavenDB" };
                var company2 = new Company { Name = "App" };
                var company3 = new Company { Name = "Micro" };
                var company4 = new Company { Name = "Amaz" };
                var company5 = new Company { Name = "dd1" };
                var company6 = new Company { Name = "dd3" };

                session.Store(employee1);
                session.Store(employee2);
                session.Store(company1, "Companies/Raven");
                session.Store(company2, "Companies/App");
                session.Store(company3, "Companies/Micro");
                session.Store(company4, "Companies/Amaz");
                session.Store(company5, "Companies/dd1");
                session.Store(company6, "Companies/dd3");

                session.SaveChanges();
            }
            new Employees_ByFirstName().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        private void InitializeData2(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {

                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }
        }

        private void AssertIncludedDocsAndRql(IDocumentSession session, string[] expectedKeys, string actualRql, string expectedRql)
        {
            var includedDocs = ((DocumentSession)session).IncludedDocumentsById;
            Assert.Equal(expectedKeys.Length, includedDocs.Count);
            foreach (var key in expectedKeys)
            {
                Assert.Contains(key, includedDocs.Keys);
            }
            var initialRequestCount = session.Advanced.NumberOfRequests;
            var documents = session.Load<dynamic>(expectedKeys);
            var finalRequestCount = session.Advanced.NumberOfRequests;
            Assert.Equal(initialRequestCount, finalRequestCount);
            RavenTestHelper.AssertStartsWithRespectingNewLines(expectedRql, actualRql);
        }

        private class Employees_ByFirstName : AbstractIndexCreationTask<Employee>
        {
            public Employees_ByFirstName()
            {
                Map = employees => from employee in employees
                    select new { FirstName = employee.FirstName, Company = employee.Company };
            }

            public class IndexEntry
            {
                public string Id { get; set; }
                public string FirstName { get; set; }
            }
        }


        private class Foo
        {
            public object _ { get; set; }
            public string Name { get; set; }
        }

        private class Employee
        {
            public string Id { get; set; }
            public double Number { get; set; }
            public string FirstName { get; set; }
            public string Company { get; set; }
            public string[] List { get; set; }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Address
        {
            public string CountryState { get; set; }
            public string City { get; set; }
            public string StateId;
        }

        private class State
        {
            public string Name { get; set; }
        }

        private class QueryResult
        {
            public string Comapny { get; set; }
        }
    }
}
