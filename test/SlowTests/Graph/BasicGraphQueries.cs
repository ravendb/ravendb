using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Graph
{
    public class BasicGraphQueries : RavenTestBase
    {
        private class StalenessParameters
        {
            public bool WaitForIndexing { get; set; }
            public bool WaitForNonStaleResults { get; set; }
            public TimeSpan? WaitForNonStaleResultsDuration { get; set; }

            public static readonly StalenessParameters Default = new StalenessParameters
            {
                WaitForIndexing = true,
                WaitForNonStaleResults = false,
                WaitForNonStaleResultsDuration = TimeSpan.FromSeconds(15)
            };
        }

        private List<T> Query<T>(string q, Action<IDocumentStore> mutate = null, StalenessParameters parameters = null)
        {
            if (parameters == null)
            {
                parameters = StalenessParameters.Default;
            }

            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                mutate?.Invoke(store);
                if (parameters.WaitForIndexing)
                {
                    WaitForIndexing(store);
                }

                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.RawQuery<T>(q);
                    if (parameters.WaitForNonStaleResults)
                    {
                        query = query.WaitForNonStaleResults(parameters.WaitForNonStaleResultsDuration);
                    }

                    return query.ToList();
                }
            }
        }

        private void AssertQueryResults(params (string q, int results)[] expected)
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

        private class OrderAndProduct
        {
#pragma warning disable 649
            public string OrderId;
            public string Product;
            public double Discount;
#pragma warning restore 649
        }

        [Fact]
        public void CanCacheGraphQueries()
        {
            void IssueQuery(IDocumentSession session)
            {
                var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users where id() = 'users/2'} as u
                        match (u) select u.Name").ToList().Select(x => x["Name"].Value<string>()).ToArray();

                Assert.Equal(1, results.Length);
                results[0] = "Jill";
            }

            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    IssueQuery(session);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    IssueQuery(session);
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

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
        public void CanSkipAndTake()
        {
            var results = Query<OrderAndProduct>(@"
match (Orders as o where id() = 'orders/821-A')-[Lines as l select Product]->(Products as p)
select {
    OrderId: id(o),
    Product: p.Name,
    Discount: l.Discount
}
Limit 1,1
");
            Assert.Equal(1, results.Count);
            var res = results.First();
            Assert.Equal("Ipoh Coffee", res.Product);
        }

        [Fact]
        public void CanIncludeFromJavaScriptInGraphQueries()
        {
            var rawQuery =
                        @"
declare function includeProducts(doc)
{
    var lines = doc.Lines; // avoid eval
    var length = lines.length; // avoid eval
    for (var i=0; i< length; i++)
    {
        include(lines[i].Product);
    }
    return doc;
}

match (Orders as o where id() = 'orders/821-A')
select  includeProducts(o)
";
            TestIncludeQuery(rawQuery);
        }

        [Fact]
        public void CanIncludeInGraphQueries()
        {
            var rawQuery =
                @"
match (Orders where id() = 'orders/821-A')
include Lines.Product
";
            TestIncludeQuery(rawQuery);
        }

        private void TestIncludeQuery(string rawQuery)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>(rawQuery).ToList();
                    var numberOfRequests = session.Advanced.NumberOfRequests;
                    var products = session.Load<Product>(new[] { "products/28-A", "products/43-A", "products/77-A" });
                    Assert.Equal(products.Count, 3);
                    Assert.True(products.ContainsKey("products/28-A"));
                    Assert.True(products.ContainsKey("products/43-A"));
                    Assert.True(products.ContainsKey("products/77-A"));
                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseEmptyDocumentAlias()
        {
            var results = Query<Employee>(@"
match (Employees as e where FirstName='Nancy')-[ReportsTo]->(_ as manager)
select manager
");
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Fuller", item.LastName);
            }
        }

        public class SimpleQueryResult
        {
            public Employee Employees, Boss;
            public string Employees_ReportsTo;

        }

        [Fact]
        public void CanProjectFromAnonymousAlias()
        {
            var results = Query<SimpleQueryResult>(@"
match (Employees where id() ='employees/7-A')-[ReportsTo]->(Employees as Boss)
");
            Assert.Equal(1, results.Count);
            Assert.Equal("employees/5-A", results[0].Employees_ReportsTo);
            Assert.Equal("Robert", results[0].Employees.FirstName);
            Assert.Equal("Steven", results[0].Boss.FirstName);
        }

        [Fact]
        public void CanWaitForNonStaleResults()
        {
            var results = Query<Product>(@"
with {from index 'Orders/Totals'} as o
with {from index 'Product/Search'} as p
match (o)-[Lines where PricePerUnit > 200 select Product]->(p)
select p", parameters: new StalenessParameters { WaitForIndexing = false, WaitForNonStaleResults = true, WaitForNonStaleResultsDuration = TimeSpan.FromSeconds(15) });
            Assert.Equal(24, results.Count);
        }

        [Fact]
        public void NotWaitingForNonStaleResultsShouldThrow()
        {
            Assert.Throws<TimeoutException>(() => Query<Product>(@"
with {from index 'Orders/Totals'} as o
with {from index 'Product/Search'} as p
match (o)-[Lines where PricePerUnit > 200 select Product]->(p)
select p", parameters: new StalenessParameters { WaitForIndexing = false, WaitForNonStaleResults = true, WaitForNonStaleResultsDuration = TimeSpan.FromSeconds(0) }));
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
match (Employees as e where id() = 'employees/7-A')-recursive as n (longest) { [ReportsTo as m]->(Employees as intermediary) }-[ReportsTo]->(Employees as boss)
select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
");
            Assert.Equal(1, results.Count);
            foreach (var item in results)
            {
                Assert.Equal("Andrew", item.Boss);
                Assert.Equal("Robert", item.Employee);
                Assert.Equal(new[] { "employees/5-A" }, item.MiddleManagement);
            }
        }

        [Fact]
        public void CanUseMultiHopInQueriesWithScript()
        {
            var results = Query<EmployeeRelations>(@"
match (Employees as e where id() = 'employees/7-A')-recursive as n (longest) { [ReportsTo as m]->(Employees as intermediary) }-[ReportsTo]->(Employees as boss)
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
                Assert.Equal(new[] { "Steven Buchanan" }, item.MiddleManagement);
            }
        }

        [Fact]
        public void CanHandleCyclesInGraph()
        {
            var results = Query<EmployeeRelations>(@"
match (Employees as e where id() = 'employees/7-A')-recursive as n (longest) { [ReportsTo as m]->(Employees ) }-[ReportsTo]->(Employees as boss)
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
                Assert.Equal(new[] { "employees/5-A", "employees/2-A" }, item.MiddleManagement);
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

        public class Ancestry
        {
            public string Name;
            public string Eldest;
            public string[] Parentage;
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
match (People as son where Name = 'Otho Sackville-Baggins')-recursive (0) { [Parents where Gender = 'Male' select Id]->(People as ancestor where BornAt='Shire') } -[Parents where Gender = 'Male' select Id]->(People as evil where BornAt = 'Mordor')
select son.Name as Son, evil.Name as Evil")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Longo", results[0].Evil);
                    Assert.Equal("Otho Sackville-Baggins", results[0].Son);
                }
            }
        }

        [Fact]
        public void Query_with_non_existing_collection_should_fail()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"match (FooBar)").ToList());
                }
            }
        }

        [Fact]
        public void CanRecursivelyProjectObjectAsEdge()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Ancestry>(@"
match (People as son)-recursive as ancestry (2,5,'longest') { 
    [Parents where Gender = 'Male' select Id]->(People as paternal where BornAt='Shire')
}-[Parents where Gender = 'Male' select Id]->(People as paternal0 where BornAt='Shire') 
select ancestry.paternal.Name as Parentage, son.Name, paternal0.Name as Eldest")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal(new[] { "Bungo", "Mungo" }, results[0].Parentage);
                    Assert.Equal("Balbo Baggins", results[0].Eldest);
                }
            }
        }

        public class HobbitAncestry
        {
            public string Name;
            public string[] PaternalAncestors;
        }

        public class HobbitAncestrySimple
        {
            public string Name;
            public string Parentage;
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
                    Assert.Equal(new[] { "Bungo", "Mungo" }, results[0].PaternalAncestors);

                    Assert.Equal("Bungo", results[1].Name);
                    Assert.Equal(new[] { "Mungo", "Balbo Baggins" }, results[1].PaternalAncestors);
                }
            }
        }

        [Fact]
        public void CanCustomizeRecursionBehavior_DefaultsToLazy()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<HobbitAncestry>(@"
match (People as son where id() = 'people/bilbo')-recursive as ancestry (1,5) { 
    [Parents where Gender = 'Male' select Id]->(People as paternal where BornAt='Shire') 
} 
select {
    Name: son.Name,
    PaternalAncestors: ancestry.map(a=>a.paternal.Name)
}")
.ToList();
                    Assert.Equal(1, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal(new[] { "Bungo" }, results[0].PaternalAncestors);
                }
            }
        }

        [Fact]
        public void CanCustomizeRecursionBehavior_All()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<HobbitAncestrySimple>(@"
match (People as son where id() = 'people/bilbo')-recursive as ancestry (1,5, 'all') { 
    [Parents select Id]->(People as paternal) 
} 
select {
    Name: son.Name,
    Parentage: ancestry.map(a=>a.paternal.Name).join(' | ')
}")
.ToList();
                    results.Sort((x, y) => x.Parentage.CompareTo(y.Parentage));

                    Assert.Equal(4, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal("Bungo", results[0].Parentage);

                    Assert.Equal("Bilbo Baggins", results[1].Name);
                    Assert.Equal("Bungo | Mungo", results[1].Parentage);

                    Assert.Equal("Bilbo Baggins", results[2].Name);
                    Assert.Equal("Bungo | Mungo | Balbo Baggins", results[2].Parentage);

                    Assert.Equal("Bilbo Baggins", results[3].Name);
                    Assert.Equal("Bungo | Mungo | Berylla Boffin", results[3].Parentage);
                }
            }
        }


        [Theory]
        [InlineData("lazy", new[] { "Bungo" })]
        [InlineData("shortest", new[] { "Bungo" })]
        [InlineData("longest", new[] { "Bungo", "Mungo", "Balbo Baggins" })]
        public void CanCustomizeRecursionBehavior(string behavior, string[] expected)
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<HobbitAncestry>(@"
match (People as son where id() = 'people/bilbo')-recursive as ancestry (1,5, $behavior) { 
    [Parents where Gender = 'Male' select Id]->(People as paternal where BornAt='Shire') 
} 
select {
    Name: son.Name,
    PaternalAncestors: ancestry.map(a=>a.paternal.Name)
}")
.AddParameter("$behavior", behavior)
.ToList();
                    Assert.Equal(1, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal(expected, results[0].PaternalAncestors);
                }
            }
        }


        [Fact]
        public void CanHandleFiltersInMultiHopQuery_WithParameters()
        {
            using (var store = GetDocumentStore())
            {
                SetupHobbitAncestry(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<HobbitAncestry>(@"
                        match (People as son)-recursive as ancestry ($min,$max, $type) { [Parents where Gender = 'Male' select Id]->(People as paternal where BornAt='Shire') } 
                        select ancestry.paternal.Name as PaternalAncestors, son.Name")
                        .AddParameter("min", 2)
                        .AddParameter("max", 3)
                        .AddParameter("type", "longest")
                        .ToList();
                    results.Sort((x, y) => x.Name.CompareTo(y.Name)); // we didn't implement order by yet
                    Assert.Equal(2, results.Count);
                    Assert.Equal("Bilbo Baggins", results[0].Name);
                    Assert.Equal(new[] { "Bungo", "Mungo", "Balbo Baggins" }, results[0].PaternalAncestors);

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
