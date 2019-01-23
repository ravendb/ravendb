using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12678 : RavenTestBase
    {
        [Fact]
        public void Recursive_alias_without_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                    ").ToArray();

                    //this is sanity check, the real "assert" is that the query above does not throw
                    Assert.NotEmpty(results);
                    Assert.True(results[0].ContainsKey("r"));
                }
            }
        }

        [Fact]
        public void Recursive_alias_in_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r
                    ").ToArray();

                    //this is sanity check, the real "assert" is that the query above does not throw
                    Assert.NotEmpty(results);
                    Assert.True(results[0].ContainsKey("r"));
                }
            }
        }


        [Fact]
        public void Duplicate_function_calls_have_proper_error_message()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)-[ReportsTo]->(Employees as m)
                        select id(e), id(m)
                    ").ToArray());

                    Assert.True(e.Message.Contains("duplicate", StringComparison.InvariantCultureIgnoreCase));
                    Assert.True(e.Message.Contains("id", StringComparison.InvariantCultureIgnoreCase));
                    Assert.True(e.Message.Contains("function", StringComparison.InvariantCultureIgnoreCase));
                }
            }
        }

        [Fact]
        public void Alias_of_node_inside_recursive_clause_in_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r.m
                    ").ToArray();

                    //this is sanity check, the real "assert" is that the query above does not throw
                    Assert.NotEmpty(results);
                    Assert.True(results[0].ContainsKey("m"));
                }
            }
        }

        [Fact]
        public void Alias_of_node_inside_recursive_clause_with_indexer_token_on_recursive_alias_in_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var expectedResults = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r.m
                    ").ToArray();

                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r[].m
                    ").ToArray();

                    //this is sanity check, the real "assert" is that the query above does not throw
                    Assert.NotEmpty(results);
                    Assert.Equal(expectedResults, results);
                }
            }
        }

        [Fact]
        public void Make_sure_invalid_recursive_alias_qualifier_throws_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r[ (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r[].m
                    ").ToArray());

                    Assert.True(e.Message.Contains("expected", StringComparison.InvariantCultureIgnoreCase));
                    Assert.True(e.Message.Contains("'['", StringComparison.InvariantCultureIgnoreCase));

                    e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r] (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r[].m
                    ").ToArray());

                    Assert.True(e.Message.Contains("expected", StringComparison.InvariantCultureIgnoreCase));
                    Assert.True(e.Message.Contains("']'", StringComparison.InvariantCultureIgnoreCase));
                }
            }
        }

        [Fact]
        public void Alias_of_node_inside_recursive_clause_with_indexer_token_on_recursive_alias_and_on_alias_in_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {                    
                    var expectedResults = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r.m
                    ").ToArray();

                    var results = session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e)- recursive as r[] (all) { [ReportsTo]->(Employees as m) }
                        select id(e), r[].m
                    ").ToArray();

                    //this is sanity check, the real "assert" is that the query above does not throw
                    Assert.NotEmpty(results);
                    Assert.Equal(expectedResults, results);
                }
            }
        }    
    }
}
