using System;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Graph
{
    public class VerticesFromIndexes : RavenTestBase
    {
        [Fact]
        public void Can_query_with_vertices_source_from_map_reduce_index()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with { from index 'Orders/ByCompany' } as o
                        match (o)-[Company]->(Companies as c) 
                    ").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var companiesInIndex = session.Advanced.RawQuery<JObject>(@"from index 'Orders/ByCompany' select Company").ToList();
                    var companyNames = session.Load<dynamic>(companiesInIndex.Select(x => x["Company"].Value<string>()))
                        .Select(x => (string)x.Value.Name).ToArray();
                    Assert.Equal(companyNames.Length,results.Count);

                    var companiesFetchedFromGraphQuery = results.Select(x => x["c"]["Name"].Value<string>()).ToArray();
                    foreach (var c in companyNames)
                    {
                        Assert.Contains(c, companiesFetchedFromGraphQuery);
                    }
                }
            }
        }

        [Fact]
        public void Can_query_with_vertices_source_from_map_index()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Order>(@"
                        with { from index 'Orders/Totals' order by id() desc} as o
                        match (o)-[Company]->(Companies as c)
                        select o
                    ").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var orders = session.Advanced.RawQuery<Order>(@"from Orders order by id() desc").ToList();
                    Assert.Equal(orders.Select(x => x.Id),results.Select(x => x.Id));
                }
            }
        }

        //having map/reduce index as destination vertex source makes no sense
        [Fact]
        public void Cannot_query_with_vertices_as_destination_taken_from_map_reduce_index()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() => session.Advanced.RawQuery<JObject>(@"
                        with { from index 'Orders/ByCompany' } as byCompaniesMapReduceResults
                        match (Orders as o)-[Company]->(byCompaniesMapReduceResults)
                    ").ToList());

                    Assert.IsType<InvalidOperationException>(e.InnerException);
                }
            }
        }

        //having map/reduce index as destination vertex source makes no sense
        [Fact]
        public void Cannot_query_with_vertices_as_destination_taken_from_map_reduce_index_in_multiple_hop_queries()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() => session.Advanced.RawQuery<JObject>(@"
                        with { from index 'Orders/ByCompany' } as indexQueryResults
                        match (Orders as o)-[Company]->(indexQueryResults)-[Company]->(Companies as c)
                    ").ToList());

                    Assert.IsType<InvalidOperationException>(e.InnerException);
                }
            }
        }

    }
}
