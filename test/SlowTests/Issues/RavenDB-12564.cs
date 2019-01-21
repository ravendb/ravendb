using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12564 : RavenTestBase
    {
        [Fact]
        public void Should_implicitly_define_aliases_on_nodes_with_just_collections()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"match (Employees)-[ReportsTo]->(Employees)").ToArray();

                    Assert.NotEmpty(results); //sanity check
                    Assert.Equal(4, results[0].Count); // from, edge, to and @metadata
                }
            }
        }

        [Fact]
        public void Should_conflict_on_implicitly_defined_aliases_on_nodes_with_just_collections()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"match (Employees)-[ReportsTo]->(Employees)-[ReportsTo]->(Employees as Employees_2)").ToArray());

                    Assert.True(e.Message.Contains("implicit",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("redefinition",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("Employees_2",StringComparison.OrdinalIgnoreCase));                    
                }
            }
        }

        
        [Fact]
        public void Should_conflict_on_implicitly_defined_aliases_on_edges()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"match (Employees)-[ReportsTo]->(Employees)-[ReportsTo as Employees_ReportsTo]->(Employees)").ToArray());

                    Assert.True(e.Message.Contains("implicit",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("redefinition",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("Employees_ReportsTo",StringComparison.OrdinalIgnoreCase));                    
                }
            }
        }

        [Fact]
        public void Should_throw_when_query_without_recursive_has_duplicate_aliases()
        {
            using (var store = GetDocumentStore())
            {
                CreateDogDataWithCycle(store);

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d2)-[Likes]->(Dogs as d2)-[Likes]->(d2)").ToArray());
                    
                    Assert.False(e.Message.Contains("implicit",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("duplicate",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase));
                }
            }

        }

        [Fact]
        public void Should_throw_when_query_has_the_same_aliases_inside_and_outside_recursive()
        {
            using (var store = GetDocumentStore())
            {
                CreateDogDataWithCycle(store);

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d2)-recursive as r { [Likes as l] ->(Dogs as d2) }
                        select d2.Name as dogName, r.l.Name as l, r.d2.Name as DogName2            ").ToArray());
                    
                    Assert.False(e.Message.Contains("implicit",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("duplicate",StringComparison.OrdinalIgnoreCase));
                    Assert.True(e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase));
                }
            }
        }
    }
}
