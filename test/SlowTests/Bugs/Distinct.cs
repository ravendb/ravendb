using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class Distinct : RavenTestBase
    {
        public Distinct(ITestOutputHelper output) : base(output)
        {
        }

        private readonly IndexDefinition _index = new IndexDefinition
        {
            Name = "test",
            Maps = new HashSet<string> { "from doc in docs select new { doc.Name }" },
            Fields = new Dictionary<string, IndexFieldOptions>
            {
                {"Name", new IndexFieldOptions {Storage = FieldStorage.Yes}}
            }
        };

        [Fact]
        public void CanQueryForDistinctItems()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "rahien" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(_index));

                using (var s = store.OpenSession())
                {
                    var objects = s.Advanced.DocumentQuery<dynamic>("test")
                        .WaitForNonStaleResults()
                        .SelectFields<dynamic>("Name")
                        .OrderBy("Name")
                        .Distinct()
                        .OrderBy("Name")
                        .ToList();

                    Assert.Equal(2, objects.Count);
                    Assert.Equal("ayende", objects[0].Name.ToString());
                    Assert.Equal("rahien", objects[1].Name.ToString());
                }
            }
        }

        [Fact]
        public void CanQueryForDistinctItemsUsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "rahien" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(_index));

                using (var s = store.OpenSession())
                {
                    var objects = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(o => new { o.Name }).OrderBy(o => o.Name)
                        .Select(o => new { o.Name })
                        .Distinct()
                        .ToList();

                    Assert.Equal(2, objects.Count);
                    Assert.Equal("ayende", objects[0].Name);
                    Assert.Equal("rahien", objects[1].Name);
                }
            }
        }
        [Fact]
        public void CanQueryForDistinctItemsUsingLinq_WithPaging()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "rahien" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(_index));

                using (var s = store.OpenSession())
                {
                    var objects = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .Select(o => new { o.Name })
                        .Distinct()
                        .OrderBy(o => o.Name)
                        .Skip(1)
                        .ToList();

                    Assert.Equal(1, objects.Count);
                    Assert.Equal("rahien", objects[0].Name);
                }
            }
        }
        [Fact]
        public void CanQueryForDistinctItemsAndProperlyPage()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "ayende" });
                    s.Store(new { Name = "rahien" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(_index));

                using (var s = store.OpenSession())
                {
                    var objects = s.Advanced.DocumentQuery<dynamic>("test")
                        .WaitForNonStaleResults()
                        .OrderBy("Name")
                        .Skip(1)
                        .OrderBy("Name")
                        .SelectFields<dynamic>("Name")
                        .Distinct()
                        .ToList();

                    Assert.Equal(1, objects.Count);
                    Assert.Equal("rahien", objects[0].Name.ToString());
                }
            }
        }

        [Fact]
        public void IncludeWithDistinct()
        {
            using (var store = GetDocumentStore())
            {
                new CustomersIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer
                    {
                        Location = "PT CT",
                        Occupation = "Marketing",
                        CustomerId = "1",
                        HeadingId = "2"
                    }, "Customers/2");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    QueryStatistics qs;
                    var qRes = session.Advanced.DocumentQuery<Customer>("CustomersIndex")
                        .Statistics(out qs).WhereLucene("Occupation", "Marketing")
                        .Distinct()
                        .SelectFields<Customer>("CustomerId")
                        .Take(20)
                        .Include(Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                        .ToList();
                    var cust = session.Load<Customer>("Customers/2");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void DistinctWithMapReduce()
        {
            using (var store = GetDocumentStore())
            {
                new ReducedCustomersIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer
                    {
                        Location = "PT CT",
                        Occupation = "Marketing",
                        CustomerId = "1",
                        HeadingId = "Customers/3"
                    }, "Customers/2");


                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {

                    QueryStatistics qs;
                    var qRes = session.Advanced.DocumentQuery<Customer>("ReducedCustomersIndex")
                        .Statistics(out qs).WhereLucene("Occupation", "Marketing")
                        .Distinct()
                        .SelectFields<Customer>("CustomerId")
                        .Take(20)
                        .ToList();
                    Assert.Equal(1, qs.TotalResults);
                }
            }
        }

        public class Customer
        {
            public string Location { get; set; }
            public string Occupation { get; set; }
            public string CustomerId { get; set; }
            public string HeadingId { get; set; }
        }

        public class CustomersIndex : AbstractIndexCreationTask<Customer>
        {
            public CustomersIndex()
            {
                Map = customers => from customer in customers
                                   select new { customer.Occupation, customer.CustomerId };
                Store(x => x.Occupation, FieldStorage.Yes);
                Store(x => x.CustomerId, FieldStorage.Yes);
            }
        }

        public class ReducedCustomersIndex : AbstractIndexCreationTask<Customer, ReducedCustomersIndex.Result>
        {
            public class Result
            {
                public string Occupation { get; set; }
                public string CustomerId { get; set; }
                public int Count { get; set; }
            }
            public ReducedCustomersIndex()
            {
                Map = customers => from customer in customers
                                   select new { customer.Occupation, customer.CustomerId, Count = 1 };
                Reduce = results => from result in results
                                    group result by new { result.Occupation, result.CustomerId }
                                        into g
                                    select new
                                    {
                                        Occupation = g.Key.Occupation,
                                        CustomerId = g.Key.CustomerId,
                                        Count = g.Sum(x => x.Count)
                                    };
                Store(x => x.Occupation, FieldStorage.Yes);
                Store(x => x.CustomerId, FieldStorage.Yes);
            }
        }
    }
}
