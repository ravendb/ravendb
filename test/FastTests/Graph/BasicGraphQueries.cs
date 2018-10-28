using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Graph
{
    public class BasicGraphQueries : RavenTestBase
    {
        public List<T> Query<T>(string q, Action<IDocumentStore> mutate = null)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                mutate?.Invoke(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    return s.Advanced.RawQuery<T>(q).ToList();
                }
            }
        }

        public void AssertQueryResults(params (string q, int results)[] expected)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                foreach (var item in expected)
                {
                    using (var s = store.OpenSession())
                    {
                        var results = s.Advanced.RawQuery<object>(item.q).ToList();
                        if (results.Count != item.results)
                        {
                            Assert.False(true,
                                item.q + " was suppsed to return " + item.results + " but we got " + results.Count
                            );
                        }
                    }
                }
            }
        }

        public class OrderAndProduct
        {
            public string OrderId;
            public string Product;
            public double Discount;
        }

        [Fact]
        public void Query_with_no_matches_and_select_should_return_empty_result()
        {
            using (var store = GetDocumentStore())
            {
                CreateDogDataWithoutEdges(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                            match (Dogs as a)-[Likes]->(Dogs as f)<-[Likes]-(Dogs as b)
                            select {
                                a: a,
                                f: f,
                                b: b
                            }").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Query_with_no_matches_and_without_select_should_return_empty_result()
        {
            using (var store = GetDocumentStore())
            {
                CreateDogDataWithoutEdges(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"match (Dogs as a)-[Likes]->(Dogs as f)<-[Likes]-(Dogs as b)").ToList();
                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Empty_vertex_node_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                   var results = session.Advanced.RawQuery<Movie>(@"
                        match ()-[HasRated select Movie]->(Movies as m) select m
                    ").ToList();
                    Assert.Equal(5, results.Count);
                }
            }
        }

        [Fact]
        public void Can_flatten_result_for_single_vertex_in_row()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (v)").ToList();
                    Assert.False(allVerticesQuery.Any(row => row.ContainsKey("v"))); //we have "flat" results
                }
            }
        }

        [Fact]
        public void Mutliple_results_in_row_wont_flatten_results()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (u)-[HasRated select Movie]->(m)").ToList();
                    Assert.True(allVerticesQuery.All(row => row.ContainsKey("m")));
                    Assert.True(allVerticesQuery.All(row => row.ContainsKey("u")));
                }
            }
        }


        [Fact]
        public void Can_query_without_collection_identifier()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (v)").ToList();

                    Assert.Equal(9, allVerticesQuery.Count);
                    var docTypes = allVerticesQuery.Select(x => x["@metadata"]["@collection"].Value<string>()).ToArray();

                    Assert.Equal(3, docTypes.Count(t => t == "Genres"));
                    Assert.Equal(3, docTypes.Count(t => t == "Movies"));
                    Assert.Equal(3, docTypes.Count(t => t == "Users"));
                }
            }
        }

        [Fact]
        public void Can_use_explicit_with_clause()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users} as u
                        match (u)").ToList();

                    Assert.Equal(3, results.Count);
                    var docTypes = results.Select(x => x["@metadata"]["@collection"].Value<string>()).ToArray();
                    Assert.Equal(3, docTypes.Count(t => t == "Users"));
                }
            }
        }

        [Fact]
        public void Can_filter_vertices_with_explicit_with_clause()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users where id() = 'users/2'} as u
                        match (u) select u.Name").ToList().Select(x => x["Name"].Value<string>()).ToArray();

                    Assert.Equal(1, results.Length);
                    results[0] = "Jill";
                }
            }
        }

        [Fact]
        public void FindReferences()
        {
            using (var store = GetDocumentStore())
            {
                CreateSimpleData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<JObject>(@"match (Entities as e)-[References as r]->(Entities as e2)").ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Contains(result,
                        item => item["e"].Value<string>("Name") == "A" &&
                                item["e2"].Value<string>("Name") == "B");
                    Assert.Contains(result,
                        item => item["e"].Value<string>("Name") == "B" &&
                                item["e2"].Value<string>("Name") == "C");
                    Assert.Contains(result,
                        item => item["e"].Value<string>("Name") == "C" &&
                                item["e2"].Value<string>("Name") == "A");

                }
            }
        }

        [Fact]
        public void CanProjectSameDocumentTwice()
        {
            var results = Query<OrderAndProduct>(@"
match (Orders as o where id() = 'orders/828-A')-[Lines select Product]->(Products as p)
select {
    OrderId: id(o),
    Product: p.Name
}
");
            Assert.Equal(3, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("orders/828-A", item.OrderId);
                Assert.NotNull(item.Product);
            }
        }

        [Fact]
        public void CanProjectEdges()
        {
            var results = Query<OrderAndProduct>(@"
match (Orders as o where id() = 'orders/821-A')-[Lines as l select Product]->(Products as p)
select {
    OrderId: id(o),
    Product: p.Name,
    Discount: l.Discount
}
");
            Assert.Equal(3, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("orders/821-A", item.OrderId);
                Assert.NotNull(item.Product);
                Assert.Equal(0.15d, item.Discount);
            }
        }

        [Fact]
        public void CanUseEmptyDocumentAlias()
        {
            var results = Query<Employee>(@"
match (Employees as e where FirstName='Nancy')-[ReportsTo]->(manager)
select manager
");
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Fuller", item.LastName);
            }
        }

        [Fact]
        public void CanFilterIOnEdges()
        {
            // not a theory because I want to run all queries on a single db
            AssertQueryResults(
                ("match (Orders as o where id() = 'orders/828-A')-[Lines where ProductName = 'Chang' select Product]->(Products as p)", 1),
                ("match (Orders as o where id() = 'orders/828-A')-[Lines where ProductName != 'Chang' select Product]->(Products as p)", 2),
                ("match (Orders as o where id() = 'orders/17-A')-[Lines where Discount > 0 select Product]->(Products as p)", 1),
                ("match (Orders as o where id() = 'orders/17-A')-[Lines where Discount >= 0 select Product]->(Products as p)", 2),
                ("match (Orders as o where id() = 'orders/17-A')-[Lines where Discount <= 0.15 select Product]->(Products as p)", 2),
                ("match (Orders as o where id() = 'orders/17-A')-[Lines where Discount < 0.15 select Product]->(Products as p)", 1),
                ("match (Orders as o where id() = 'orders/828-A')-[Lines where ProductName in ('Spegesild', 'Chang') select Product]->(Products as p)", 2),
                ("match (Orders as o where id() = 'orders/830-A')-[Lines where Discount between 0 and 0.1 select Product]->(Products as p)", 24),
                ("match (Employees as e where Territories all in ('60179', '60601' ) )", 1),
                ("match (Employees as e where Territories in ('60179', '60601') )", 1)
            );
        }

        public class EmployeeRelations
        {
            public string Employee, Boss;
            public string[] MiddleManagement;
        }

        [Fact]
        public void CanUseMultiHopInQueries()
        {
            var results = Query<EmployeeRelations>(@"
match (Employees as e where id() = 'employees/7-A')-recursive as n { [ReportsTo as m] }->(Employees as boss)
select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
");
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Andrew", item.Boss);
                Assert.Equal("Robert", item.Employee);
                Assert.Equal(new[] {  "employees/5-A", "employees/2-A" }, item.MiddleManagement);
            }
        }

        [Fact]
        public void CanUseMultiHopInQueriesWithScript()
        {
            var results = Query<EmployeeRelations>(@"
match (Employees as e where id() = 'employees/7-A')-recursive as n { [ReportsTo as m] }->(Employees as boss)
select {
    Employee: e.FirstName + ' ' + e.LastName,
    MiddleManagement: n.map(f => load(f.m)).map(f => f.FirstName + ' ' + f.LastName),
    Boss: boss.FirstName + ' ' + boss.LastName
}
");
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Andrew Fuller", item.Boss);
                Assert.Equal("Robert King", item.Employee);
                Assert.Equal(new[] { "Steven Buchanan", "Andrew Fuller" }, item.MiddleManagement);
            }
        }

        [Fact]
        public void CanHandleCyclesInGraph()
        {
            var results = Query<EmployeeRelations>(@"
match (Employees as e where id() = 'employees/7-A')-recursive as n { [ReportsTo as m] }->(Employees as boss)
select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
", store =>
            {
                using (var s = store.OpenSession())
                {
                    var e = s.Load<Employee>("employees/2-A");
                    e.ReportsTo = e.Id;
                    s.SaveChanges();
                }
            });
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Andrew", item.Boss);
                Assert.Equal("Robert", item.Employee);
                Assert.Equal(new[] { "employees/5-A" }, item.MiddleManagement);
            }
        }

        public class Person
        {
            public class Parent
            {
                public string Id;
                public string Gender;
            }

            public Parent[] Parents;
            public string BornAt;
            public string Name;
        }

        public class Tragedy
        {
            public string Evil;
            public string Son;
        }

        [Fact]
        public void CanHandleFiltersInMultiHopQueryWithEndNode()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Tragedy>(@"
match (People as son where Name = 'Otho Sackville-Baggins')-recursive (0) { [Parents where Gender = 'Male' select Id]->(People as ancestor where BornAt='Shire')-[Parents where Gender = 'Male' select Id] } ->(People as evil where BornAt = 'Mordor')
select son.Name as Son, evil.Name as Evil")
                        .ToList();

                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, results.Count);
                    Assert.Equal("Longo", results[0].Evil);
                    Assert.Equal("Otho Sackville-Baggins", results[0].Son);
                }
            }
        }

        public class HobbitAncestry
        {
            public string Name;
            public string[] PaternalAncestors;
        }

        [Fact]
        public void CanHandleFiltersInMultiHopQuery()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<HobbitAncestry>(@"
match (People as son)-recursive as ancestry (2) { [Parents where Gender = 'Male' select Id]->(People as paternal where BornAt='Shire') } 
select ancestry.paternal.Name as PaternalAncestors, son.Name")
.ToList();
                    results.Sort((x, y) => x.Name.CompareTo(y.Name)); // we didn't implement order by yet
                    Assert.Equal(2, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal(new[] {"Bungo","Mungo"}, results[0].PaternalAncestors);

                    Assert.Equal("Bungo", results[1].Name);
                    Assert.Equal(new[] { "Mungo", "Balbo Baggins" }, results[1].PaternalAncestors);
                }
            }
        }

        private static void SetupHobbitAncestry(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Person
                {
                    Name = "Balbo Baggins",
                    BornAt = "Shire",
                }, "people/balbo");

                session.Store(new Person
                {
                    Name = "Berylla Boffin",
                    BornAt = "Shire",
                }, "people/berylla");


                session.Store(new Person
                {
                    Name = "Mungo",
                    BornAt = "Shire",
                    Parents = new[]
                    {
                            new Person.Parent{Gender = "Male", Id = "people/balbo"},
                            new Person.Parent{Gender = "Female", Id = "people/berylla"},
                        }
                }, "people/mungo");

                session.Store(new Person
                {
                    Name = "Longo",
                    BornAt = "Mordor",
                    Parents = new[]
                    {
                            new Person.Parent{Gender = "Male", Id = "people/balbo"},
                            new Person.Parent{Gender = "Female", Id = "people/berylla"},
                        }
                }, "people/longo");

                session.Store(new Person
                {
                    Name = "Otho Sackville-Baggins",
                    BornAt = "Shire",
                    Parents = new[]
                    {
                            new Person.Parent{Gender = "Male", Id = "people/longo"},
                            new Person.Parent{Gender = "Female", Id = "people/camellia"},
                        }
                }, "people/otho");

                session.Store(new Person
                {
                    Name = "Bungo",
                    BornAt = "Shire",
                    Parents = new[]
                   {
                            new Person.Parent{Gender = "Male", Id = "people/mungo"},
                            new Person.Parent{Gender = "Female", Id = "people/laura"},
                        }
                }, "people/bungo");

                session.Store(new Person
                {
                    Name = "Bilbo Baggins",
                    BornAt = "Shire",
                    Parents = new[]
                  {
                            new Person.Parent{Gender = "Male", Id = "people/bungo"},
                            new Person.Parent{Gender = "Female", Id = "people/belladonna"},
                        }
                }, "people/bilbo");

                session.SaveChanges();
            }
        }
    }
}
