using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Graph
{
    public class BasicGraphQueries : RavenTestBase
    {
        public BasicGraphQueries(ITestOutputHelper output) : base(output)
        {
        }

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

        private List<T> Query<T>(Options options, string q, Action<IDocumentStore> mutate = null, StalenessParameters parameters = null)
        {
            if (parameters == null)
            {
                parameters = StalenessParameters.Default;
            }

            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                mutate?.Invoke(store);
                if (parameters.WaitForIndexing)
                {
                    Indexes.WaitForIndexing(store);
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

        private void AssertQueryResults(Options options, params (string q, int results)[] expected)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                Indexes.WaitForIndexing(store);

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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanCacheGraphQueries(Options options)
        {
            void IssueQuery(IDocumentSession session)
            {
                var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users where id() = 'users/2'} as u
                        match (u) select u.Name").ToList().Select(x => x["Name"].Value<string>()).ToArray();

                Assert.Equal(1, results.Length);
                results[0] = "Jill";
            }

            using (var store = GetDocumentStore(options))
            {
                Samples.CreateMoviesData(store);
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanProjectSameDocumentTwice(Options options)
        {
            var results = Query<OrderAndProduct>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanProjectEdges(Options options)
        {
            var results = Query<OrderAndProduct>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanSkipAndTake(Options options)
        {
            var results = Query<OrderAndProduct>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanIncludeFromJavaScriptInGraphQueries(Options options)
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
            TestIncludeQuery(options, rawQuery);
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanIncludeInGraphQueries(Options options)
        {
            var rawQuery =
                @"
match (Orders where id() = 'orders/821-A')
include Lines.Product
";
            TestIncludeQuery(options, rawQuery);
        }

        private void TestIncludeQuery(Options options, string rawQuery)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var _ = session.Advanced.RawQuery<Order>(rawQuery).ToList();
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void Can_filter_source_node(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Samples.CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var movieIds = session.Advanced.RawQuery<JObject>(@"
                        match(Users as u where id() = 'users/3')-[HasRated.Movie]->(Movies as m)
                        select id(m) as MovieId
                    ").ToArray().Select(x => x["MovieId"].Value<string>()).Distinct().ToArray();

                    Assert.Contains("movies/3", movieIds);
                    Assert.Equal(1, movieIds.Length);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void Can_filter_destination_node(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Samples.CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var userIds = session.Advanced.RawQuery<JObject>(@"
                        match(Users as u)-[HasRated.Movie]->(Movies as m where id() = 'movies/2')
                        select id(u) as UserId
                    ").ToArray().Select(x => x["UserId"].Value<string>()).Distinct().ToArray();

                    Assert.DoesNotContain("users/3", userIds); //only user with Id == 'users/3' didn't rate 'movies/2'
                    Assert.Equal(2, userIds.Length);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void Can_filter_edge(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Samples.CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var userIds = session.Advanced.RawQuery<JObject>(@"
                        match(Users as u)-[HasRated where Movie = 'movies/2' select Movie]->(Movies as m)
                        select id(u) as UserId
                    ").ToArray().Select(x => x["UserId"].Value<string>()).Distinct().ToArray();

                    Assert.DoesNotContain("users/3", userIds); //only user with Id == 'users/3' didn't rate 'movies/2'
                    Assert.Equal(2, userIds.Length);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanUseEmptyDocumentAlias(Options options)
        {
            var results = Query<Employee>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanProjectFromAnonymousAlias(Options options)
        {
            var results = Query<SimpleQueryResult>(options, @"
match (Employees where id() ='employees/7-A')-[ReportsTo]->(Employees as Boss)
");
            Assert.Equal(1, results.Count);
            Assert.Equal("employees/5-A", results[0].Employees_ReportsTo);
            Assert.Equal("Robert", results[0].Employees.FirstName);
            Assert.Equal("Steven", results[0].Boss.FirstName);
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanUseDistinctInGraphQueries(Options options)
        {
            var results = Query<Product>(options, @"
match (Orders as o)-[Lines select Product]->(Products)
select distinct Products");
            Assert.Equal(77, results.Count);
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanWaitForNonStaleResults(Options options)
        {
            var results = Query<Product>(options,
                @"
with {from index 'Orders/Totals'} as o
with {from index 'Product/Search'} as p
match (o)-[Lines where PricePerUnit > 200 select Product]->(p)
select p", parameters: new StalenessParameters { WaitForIndexing = false, WaitForNonStaleResults = true, WaitForNonStaleResultsDuration = TimeSpan.FromSeconds(15) });
            Assert.Equal(24, results.Count);
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void NotWaitingForNonStaleResultsShouldThrow(Options options)
        {
            Assert.Throws<TimeoutException>(() => Query<Product>(options,
                @"
with {from index 'Orders/Totals'} as o
with {from index 'Product/Search'} as p
match (o)-[Lines where PricePerUnit > 200 select Product]->(p)
select p", mutate: (store) =>
            {
                // we need to reset those indexes, since our sample data is larger now and they will be non-stale until the import is completed
                store.Maintenance.Send(new ResetIndexOperation("Orders/Totals"));
                store.Maintenance.Send(new ResetIndexOperation("Product/Search"));
            }, parameters: new StalenessParameters { WaitForIndexing = false, WaitForNonStaleResults = true, WaitForNonStaleResultsDuration = TimeSpan.FromSeconds(0) }));
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanFilterIOnEdges(Options options)
        {
            // not a theory because I want to run all queries on a single db
            AssertQueryResults(options,
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanUseMultiHopInQueries(Options options)
        {
            var results = Query<EmployeeRelations>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanUseMultiHopInQueriesWithScript(Options options)
        {
            var results = Query<EmployeeRelations>(options, @"
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanHandleCyclesInGraph(Options options)
        {
            var results = Query<EmployeeRelations>(options, @"
match (Employees as e where id() = 'employees/7-A')-recursive as n (longest) { [ReportsTo as m]->(Employees ) }-[ReportsTo]->(Employees as boss)
select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
", store =>
            {
                using (var s = store.OpenSession())
                {
                    //add self-cycle at "employees/2-A"
                    var e = s.Load<Employee>("employees/2-A");
                    e.ReportsTo = e.Id;
                    s.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanHandleFiltersInMultiHopQueryWithEndNode(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void Query_with_non_existing_collection_should_fail(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"match (FooBar)").ToList());
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanRecursivelyProjectObjectAsEdge(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanHandleFiltersInMultiHopQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanCustomizeRecursionBehavior_DefaultsToLazy(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanCustomizeRecursionBehavior_All(Options options)
        {
            using (var store = GetDocumentStore(options))
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
            var confg = new RavenTestParameters()
            {
                SearchEngine = RavenSearchEngineMode.Lucene
            };
            
            using (var store = GetDocumentStore(Options.ForSearchEngine(confg)))
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


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanHandleFiltersInMultiHopQuery_WithParameters(Options options)
        {
            using (var store = GetDocumentStore(options))
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
