using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Graph
{
    public class BasicGraphQueries : RavenTestBase
    {
        
        public class Employee
        {
            public string Id { get; set; }
            public string LastName { get; set; }
            public string FirstName { get; set; }
            public string Title { get; set; }
            public Address Address { get; set; }
            public DateTime HiredAt { get; set; }
            public DateTime Birthday { get; set; }
            public string HomePhone { get; set; }
            public string Extension { get; set; }
            public string ReportsTo { get; set; }
            public List<string> Notes { get; set; }
            public List<string> Territories { get; set; }
        }

        public class Contact
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }
        
        public class Address
        {
            public string Line1 { get; set; }
            public string Line2 { get; set; }
            public string City { get; set; }
            public string Region { get; set; }
            public string PostalCode { get; set; }
            public string Country { get; set; }
            public Location Location { get; set; }
        }

        public class Location
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        public class Company
        {
            public string Id { get; set; }
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public Contact Contact { get; set; }
            public Address Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
        }

        public List<T> Query<T>(string q)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

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
        public void Query_should_support_parameterless_id_function_in_select()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"match (o:Orders) select id()").ToList()
                        .Select(x => x["id()"].Value<string>()).OrderBy(x => x);
                    Assert.NotEmpty(results);

                    var orders = session.Query<Order>().ToList().Select(x => x.Id).OrderBy(x => x);
                    Assert.Equal(orders,results);
                }
            }
        }

        [Fact]
        public void Query_should_support_id_function_in_select()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var orderIdsFromGraphQuery = session.Advanced.RawQuery<JObject>(@"
                            match (o:Orders) select { id: id(o) }").ToList()
                        .Select(x => x["id"].Value<string>()).OrderBy(x => x);
                    Assert.NotEmpty(orderIdsFromGraphQuery);

                    var ordersIsFromDocumentQuery = session.Query<Order>().ToList().Select(x => x.Id).OrderBy(x => x);
                    Assert.Equal(ordersIsFromDocumentQuery,orderIdsFromGraphQuery);
                }
            }
        }

        [Fact]
        public void Query_should_support_multiple_id_functions_in_select()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (o:Orders)-[:Company]->(c:Companies)
                        select 
                        { 
                            OrderId: id(o),
                            CompanyId : id(c)
                        }").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var orderIds = results.Select(x => x["OrderId"].Value<string>()).Distinct().OrderBy(x => x);
                    var companyIds = results.Select(x => x["CompanyId"].Value<string>()).OrderBy(x => x);

                    var ordersIsFromDocumentQuery = session.Query<Order>().ToList().Select(x => x.Id).OrderBy(x => x);
                    Assert.Equal(ordersIsFromDocumentQuery,orderIds);

                    var companyIsFromDocumentQuery = session.Query<Order>().ToList().Select(x => x.Company).OrderBy(x => x);
                    Assert.Equal(companyIsFromDocumentQuery,companyIds);
                }
            }
        }

        [Fact]
        public void Query_should_support_id_function_in_vertex_filter()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (o:Orders(id() = 'orders/829-a'))-[:Company]->(c:Companies)").ToList();

                    Assert.Equal(1,results.Count);
                }
            }
        }

        //we do not allow id() usage when there is more than one possible targets for it
            //in this case id(o) and id(c) should be used
            [Fact]
        public void Query_with_parameterless_id_function_in_select_and_multiple_aliases_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() => 
                        session.Advanced.RawQuery<JObject>(@"
                            match (o:Orders)-[:Company]->(c:Companies) select id()").ToList());
                    
                    Assert.IsType<InvalidOperationException>(e.InnerException);
                }
            }
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
                            match (a:Dogs)-[:Likes]->(f:Dogs)<-[:Likes]-(b:Dogs)
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
                    var results = session.Advanced.RawQuery<JObject>(@"match (a:Dogs)-[:Likes]->(f:Dogs)<-[:Likes]-(b:Dogs)").ToList();
                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Empty_vertex_node_should_fail_the_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<Movie>(@"
                        match ()-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList());
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
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (u)-[:HasRated.Movie]->(m)").ToList();
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
                    var result = session.Advanced.RawQuery<JObject>(@"match (e:Entities)-[:References]->(e2:Entities)").ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Contains(result,
                        item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/1" &&
                                item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/2");
                    Assert.Contains(result,
                        item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/2" &&
                                item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/3");
                    Assert.Contains(result,
                        item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/3" &&
                                item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/1");
                }
            }
        }

        [Fact]
        public void CanProjectSameDocumentTwice()
        {
            var results = Query<OrderAndProduct>(@"
match (o:Orders (id() = 'orders/828-A'))-[:Lines.Product]->(p:Products)
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
match (o:Orders (id() = 'orders/821-A'))-[l:Lines.Product]->(p:Products)
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
match (e:Employees(FirstName='Nancy'))-[:ReportsTo]->(manager)
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
                ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName = 'Chang').Product]->(p:Products)", 1),
                ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName != 'Chang').Product]->(p:Products)", 2),
                ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount > 0).Product]->(p:Products)", 1),
                ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount >= 0).Product]->(p:Products)", 2),
                ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount <= 0.15).Product]->(p:Products)", 2),
                ("match (o:Orders (id() = 'orders/17-A'))-[:Lines(Discount < 0.15).Product]->(p:Products)", 1),
                ("match (o:Orders (id() = 'orders/828-A'))-[:Lines(ProductName in ('Spegesild', 'Chang') ).Product]->(p:Products)", 2),
                ("match (o:Orders (id() = 'orders/830-A'))-[:Lines(Discount between 0 and 0.1).Product]->(p:Products)", 24),
                ("match( e: Employees(Territories all in ('60179', '60601') ) )", 1),
                ("match(e: Employees(Territories in ('60179', '60601')) )", 1)
            );

        }
    }
}
