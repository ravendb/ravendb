using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Graph
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
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (_ as v)").ToList();
                    Assert.False(allVerticesQuery.Any(row => row.ContainsKey("_ as v"))); //we have "flat" results
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
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (_ as u)-[HasRated select Movie]->(_ as m)").ToList();
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
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (_ as v)").ToList();

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
    }
}
