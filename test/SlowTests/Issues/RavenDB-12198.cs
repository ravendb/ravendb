using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12198 : RavenTestBase
    {
        [Fact]
        public void Missing_as_keyword_should_properly_throw_in_non_recursive_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                       match (Users u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                    ").ToArray());

                    Assert.True(e.Message.Contains("invalid",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("u1",StringComparison.OrdinalIgnoreCase));
                }
            }
        }

        [Fact]
        public void Select_in_node_without_alias_should_properly_throw()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                       match (Users select Name)-[HasRated select Movie]->(Movies as m)
                    ").ToArray());

                    Assert.True(e.Message.Contains("select",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("forbidden",StringComparison.OrdinalIgnoreCase));
                }
            }
        }

        //  match (Employees where id() = 'employees/7-A')- recursive as n { [ReportsTo as m]->(Employees as boss) }
        //select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
        [Fact]
        public void Missing_as_keyword_should_properly_throw_in_recursive_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees as e where id() = 'employees/7-A')- recursive as n { [ReportsTo as m]->(Employees boss) }
                        select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
                    ").ToArray());

                    Assert.True(e.Message.Contains("invalid",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("boss",StringComparison.OrdinalIgnoreCase));
                }
            }
        }

        [Fact]
        public void Missing_as_keyword_with_where_should_properly_throw_in_recursive_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(@"
                        match (Employees e where id() = 'employees/7-A')- recursive as n { [ReportsTo as m]->(Employees as boss) }
                        select e.FirstName as Employee, n.m as MiddleManagement, boss.FirstName as Boss
                    ").ToArray());
                    Assert.True(e.Message.Contains("invalid",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("alias",StringComparison.OrdinalIgnoreCase) && e.Message.Contains("e",StringComparison.OrdinalIgnoreCase));
                }
            }
        }
    }
}
