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
        public void IncludeWithMemberInitMethodSyntax()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Address>()
                        .Select(a => new
                        {
                            _ = RavenQuery.Include<Address>(a => a.StateId),
                            a.StateId
                        });

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select StateId include StateId");

                    var typedProjection = session.Query<Address>()
                        .Select(a => new Foo { _ = RavenQuery.Include<Address>(a => a.StateId), StateId = a.StateId, });

                    _ = typedProjection.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        typedProjection.ToString(),
                        "from 'Addresses' select StateId include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithMemberInitQuerySyntax()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(a => a.StateId)
                        select new
                        {
                            StateId = a.StateId,
                        };

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select StateId include StateId");

                    var typedProjection = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(a => a.StateId)
                        select new Foo
                        {
                            StateId = a.StateId
                        };

                    _ = typedProjection.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        typedProjection.ToString(),
                        "from 'Addresses' select StateId include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void IncludeWithMemberInitAndProjectedFieldMethodSyntax()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Address>()
                        .Select(a => new
                        {
                            _ = RavenQuery.Include<Address>(x => x.StateId),
                            Name = a.City,
                            StateId = a.StateId
                        });

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select City as Name, StateId include StateId");

                    var typedProjection = session.Query<Address>()
                        .Select(a => new Foo
                        {
                            _ = RavenQuery.Include<Address>(x => x.StateId),
                            Name = a.City,
                            StateId = a.StateId
                        });

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        typedProjection.ToString(),
                        "from 'Addresses' select City as Name, StateId include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void IncludeWithMemberInitAndProjectedFieldQuerySyntax()
        {
            using (DocumentStore store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.StateId)
                        select new
                        {
                            Name = a.City,
                            StateId = a.StateId
                        };

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select City as Name, StateId include StateId");

                    var typedProjection = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.StateId)
                        select new Foo
                        {
                            Name = a.City,
                            StateId = a.StateId
                        };

                    _ = typedProjection.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        typedProjection.ToString(),
                        "from 'Addresses' select City as Name, StateId include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void SelectAndProjectionWithInclude()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<Address>()
                        .Select(a => new
                        {
                            Name = a.City + a.StateId,
                            StateId = a.StateId,
                            _ = RavenQuery.Include<Address>(a => a.StateId)
                        });

                    _ = query1.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query1.ToString(),
                        "from 'Addresses' as a select { Name : a.City+a.StateId, StateId : a.StateId } include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeTwoSelect()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = from e in session.Query<Address>()
                                let _ = RavenQuery.Include<Address>(a => a.StateId)
                                let __ = RavenQuery.Include<Address>(e => e.Company)
                                select new
                                {
                                    City = e.City,
                                    StateId = e.StateId,
                                    Company = e.Company,
                                };

                    var result = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/Raven", "Companies/App", "states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select City, StateId, Company include StateId,Company");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeInsideAndOutsideSelect()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = from e in session.Query<Address>()
                                let _ = RavenQuery.Include<Address>(a => a.StateId) // Outside select
                                select new
                                {
                                    City = e.City,
                                    StateId = e.StateId,
                                    Company = e.Company,
                                    _ = RavenQuery.Include<Address>(e => e.Company) // Inside select
                                };

                    var result = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/Raven", "Companies/App", "states/1", "states/2"],
                        query.ToString(),
                        "from 'Addresses' select City, StateId, Company include StateId,Company");
                }
            }
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
                        .Select(a => new Foo
                        {
                            _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]),
                            Name = a.FirstName
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
        public void IncludeWithMemberInitSplitTestQueryStyle()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query = from e in session.Query<Address>()
                        let _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0])
                        let __ = RavenQuery.Include<Address>(a => a.StateId)
                        select new
                        {
                            Company = e.Company,
                            StateId = e.StateId
                        };

                    _ = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/App", "Companies/Raven", "states/1", "states/2"],
                        query.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Company : e.Company, StateId : e.StateId };\r\n}\r\nfrom 'Addresses' as e select output(e) include StateId");
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithMemberInitSplitTestQueryStyleWithTypedProjection()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query1 = from e in session.Query<Employee, Employees_ByFirstName>()
                                let _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0])
                                let __ = RavenQuery.Include<Address>(a => a.StateId)
                                select new Foo
                                {
                                    Name = e.FirstName,
                                };
                    
                    _ = query1.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/App", "Companies/Raven"],
                        query1.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Name : e.FirstName };\r\n}\r\nfrom index 'Employees/ByFirstName' as e select output(e) include StateId");
                }
            }
        }

        

        [RavenFact(RavenTestCategory.Querying)]
        public void IncludeWithSingleProperty()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query3 = from e in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(a => a.StateId)
                        select e.StateId;

                    var results3 = query3.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query3.ToString(),
                        "from 'Addresses' select StateId include StateId"
                    );
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
                        select new
                        {
                            FirstName = e.FirstName
                        };

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
        public void IncludeInsideAndOutsideSelectComplex()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData1(store);

                using (var session = store.OpenSession())
                {
                    var query = from e in session.Query<Employee, Employees_ByFirstName>()
                         let _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[0]) // Outside select
                        select new
                        {
                            FirstName = e.FirstName,
                            _ = RavenQuery.Include<Employee>(a => a.Company.Split('#', StringSplitOptions.None)[1]) // Inside select
                        };

                   var result = query.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["Companies/Amaz", "Companies/App", "Companies/Raven", "Companies/App"],
                        query.ToString(),
                        "declare function output(e) {\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\tinclude(e.Company.split(new RegExp(\"#\", \"g\"))[1]);\r\n\treturn { FirstName : e.FirstName };\r\n}\r\nfrom index 'Employees/ByFirstName' as e select output(e)");
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
        public void NewTest()
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


        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithInvalidName2()
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
                            Includy = Raven.Client.Documents.Queries.RavenQuery.Include<Employee>(a => a.Company)
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
        public void SessionQuerySelectAddressFromIncludeDoc_UsingRavenQueryWithComplexLambdaExpression()
        { 
            using (DocumentStore store = GetDocumentStore())
            {
              InitializeData2(store);

                using (var session = store.OpenSession())
                {
                    var query3 = from a in session.Query<Address>()
                        let _ = RavenQuery.Include<Address>(x => x.CountryState.Split('#', StringSplitOptions.None)[0])
                        select new
                        {
                            Name = a.City
                        };

                    var res2 = query3.ToList();

                    AssertIncludedDocsAndRql(
                        session,
                        ["states/1", "states/2"],
                        query3.ToString(),
                        "declare function output(a) {\r\n\tinclude(a.CountryState.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { Name : a.City };\r\n}\r\nfrom 'Addresses' as a select output(a)");
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

                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york", StateId = "states/1",
                    Company = "Companies/Raven"});
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa", StateId = "states/2",
                    Company = "Companies/App" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");
                var company1 = new Company { Name = "RavenDB" };
                var company2 = new Company { Name = "App" };
                var company3 = new Company { Name = "Micro" };
                var company4 = new Company { Name = "Amaz" };
                session.Store(company1, "Companies/Raven");
                session.Store(company2, "Companies/App");
                session.Store(company3, "Companies/Micro");
                session.Store(company4, "Companies/Amaz");

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
            public string StateId { get; set; }
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
            public string StateId { get; set; }
            public string Company { get; set; }
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
