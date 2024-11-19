using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;


namespace SlowTests.Client.Queries
{
    public class RavenDB_14541 : RavenTestBase
    {
        public RavenDB_14541(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void IncludeWithSingleSplitTest()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData(store);

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(a => new
                        {
                            FirstName = a.FirstName, _ = Raven.Client.Documents.Queries.RavenQuery.Include(a.Company.Split('#', StringSplitOptions.None)[0]),
                        });

                    var query1String = query1.ToString();
                    var results = query1.ToList();

                    Assert.Equal("from index 'Employees/ByFirstName' as a select { FirstName : a.FirstName, _ : include(a.Company.split(new RegExp(\"#\", \"g\"))[0]) }",
                        query1String);

                    var includedDocs = ((DocumentSession)session).IncludedDocumentsById;
                    Assert.Equal(2, includedDocs.Count);
                    Assert.Equal("Companies/App", includedDocs.First().Key);
                    Assert.Equal("Companies/Raven", includedDocs.Last().Key);

                    var numOfReq = session.Advanced.NumberOfRequests;
                    var documents = session.Load<Company>(["Companies/Raven", "Companies/App"]);
                    Assert.Equal(numOfReq, session.Advanced.NumberOfRequests);
                }
            }
        }


        public void IncludeWithLet()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData(store);

                using (var session = store.OpenSession())
                {
                    var query2 = from e in session.Query<Employee>()
                        let _ = RavenQuery.Include(e.Company.Split('#', StringSplitOptions.None)[0])
                        select new { FirstName = e.FirstName };

                    var query2String = query2.ToString();
                    var results2 = query2.ToList();

                    var includedDocs2 = ((DocumentSession)session).IncludedDocumentsById;
                    Assert.Equal(
                        "declare function output(e) {\r\n\tvar _ = include(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn { FirstName : e.FirstName };\r\n}\r\nfrom 'Employees' as e select output(e)",
                        query2String);
                    Assert.Equal(2, includedDocs2.Count);
                    Assert.Equal("Companies/App", includedDocs2.First().Key);
                    Assert.Equal("Companies/Raven", includedDocs2.Last().Key);

                    var numOfReq2 = session.Advanced.NumberOfRequests;
                    var documents2 = session.Load<Company>(new[] { "Companies/Raven", "Companies/App" });
                    Assert.Equal(numOfReq2, session.Advanced.NumberOfRequests);
                }
            }
        }


        public void IncludeWithSinglePropertyAndLet()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData(store);

                using (var session = store.OpenSession())
                {
                    var query3 = from e in session.Query<Employee>()
                        let _ = RavenQuery.Include(e.Company.Split('#', StringSplitOptions.None)[0])
                        select e.FirstName;

                    var results3String = query3.ToString();
                    var results3 = query3.ToList();

                    var includedDocs3 = ((DocumentSession)session).IncludedDocumentsById;
                    Assert.Equal(2, includedDocs3.Count);
                    Assert.Equal(
                        "declare function output(e) {\r\n\tvar _ = include(e.Company.split(new RegExp(\"#\", \"g\"))[0]);\r\n\treturn {FirstName:e.FirstName};\r\n}\r\nfrom 'Employees' as e select output(e)",
                        results3String);
                    Assert.NotEmpty(results3);

                    var numOfReq3 = session.Advanced.NumberOfRequests;
                    var documents3 = session.Load<Company>(new[] { "Companies/Raven", "Companies/App" });
                    Assert.Equal(numOfReq3, session.Advanced.NumberOfRequests);
                }
            }
        }



        public void MultipleIncludesInsideSelect()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData(store);

                using (var session = store.OpenSession())
                {
                    var query4 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(a => new
                        {
                            FirstName = a.FirstName,
                            _ = Raven.Client.Documents.Queries.RavenQuery.Include(a.Company.Split('#', StringSplitOptions.None)[0]),
                            __ = Raven.Client.Documents.Queries.RavenQuery.Include(a.Company.Split('#', StringSplitOptions.None)[1])
                        });

                    var results4 = query4.ToList();
                    var includedDocs4 = ((DocumentSession)session).IncludedDocumentsById;

                    Assert.Equal(4, includedDocs4.Count);

                    var keysList = includedDocs4.Keys.ToHashSet();

                    Assert.Contains("Companies/Amaz", keysList);
                    Assert.Contains("Companies/App", keysList);
                    Assert.Contains("Companies/Micro", keysList);
                    Assert.Contains("Companies/Raven", keysList);
                    Assert.NotEmpty(results4);
                }
            }
        }


        public void IncludeWithInvalidName()
        {
            using (var store = GetDocumentStore())
            {
                InitializeData(store);

                using (var session = store.OpenSession())
                {
                    var query5 = session.Query<Employee, Employees_ByFirstName>()
                        .Select(a => new
                        {
                            FirstName = a.FirstName,
                            _ = Raven.Client.Documents.Queries.RavenQuery.Include(a.Company.Split('#', StringSplitOptions.None)[0]),
                            Include = Raven.Client.Documents.Queries.RavenQuery.Include(a.Company.Split('#', StringSplitOptions.None)[1])
                        });

                    var error = Assert.Throws<InvalidOperationException>(() => query5.ToList());
                    Assert.Equal("The include variable can only be assigned to the discard character (_)", error.Message);
                }
            }
        }

        private void InitializeData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var employee1 = new Employee { FirstName = "Golan", Company = "Companies/Raven#Companies/Micro" };
                var employee2 = new Employee { FirstName = "Grisha", Company = "Companies/App#Companies/Amaz" };
                var company1 = new Company { Name = "RavenDB" };
                var company2 = new Company { Name = "App" };
                var company3 = new Company { Name = "Micro" };
                var company4 = new Company { Name = "Amaz" };

                session.Store(employee1);
                session.Store(employee2);
                session.Store(company1, "Companies/Raven");
                session.Store(company2, "Companies/App");
                session.Store(company3, "Companies/Micro");
                session.Store(company4, "Companies/Amaz");

                session.SaveChanges();
            }

            new Employees_ByFirstName().Execute(store);
            Indexes.WaitForIndexing(store);
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

        private class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string Company { get; set; }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
