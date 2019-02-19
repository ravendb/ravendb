using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12061 : RavenTestBase
    {
        [Fact]
        public void Can_parse_intersections_with_parenthesis()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>("match (((Employees as e1)) or ((Employees as e2))) or (Employees as e3)").ToArray();
                    Assert.Equal(900,results.Length);
                }
            }
        }

        [Fact]
        public void Throws_proper_exception_on_missing_closing_bracket()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines[.Product]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("Lines") && e.Message.Contains("]"));
                }
            }
        }
        
        [Fact]
        public void Throws_proper_exception_on_missing_field_after_dot()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines[].]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("Lines") && e.Message.Contains("]") && e.Message.Contains("field"));

                    e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines.]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("Lines") && e.Message.Contains("]") && e.Message.Contains("field"));
                }
            }
        }

        [Fact]
        public void Throws_proper_exception_on_missing_opening_bracket()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines].Product]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("Lines") && e.Message.Contains("."));

                    var e2 = Assert.Throws<RavenException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines]]->(Products as p)
                        ").ToArray());                

                    
                    Assert.True(e2.Message.Contains("]"));

                    e2 = Assert.Throws<RavenException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A'))-[Lines]->(Products as p)
                        ").ToArray());                

                    Assert.True(e2.Message.Contains(")"));
                }
            }
        }

        [Fact]
        public void Throws_proper_exception_on_missing_where_clause_condition_expression()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    //missing where expression in edge
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id() = 'orders/825-A')-[Lines.Product where]->(Products as p)
                        ").ToArray());

                    Assert.True(e.Message.Contains("Lines.Product") && 
                                e.Message.Contains("where",StringComparison.OrdinalIgnoreCase) && 
                                e.Message.Contains("filter expression",StringComparison.OrdinalIgnoreCase));

                    //missing where expression in node
                    e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where)-[Lines[].Product]->(Products as p)
                        ").ToArray());

                    Assert.True(e.Message.Contains("Orders") && 
                                e.Message.Contains("where",StringComparison.OrdinalIgnoreCase) && 
                                e.Message.Contains("filter expression",StringComparison.OrdinalIgnoreCase));
                }
            }
        }

        [Fact]
        public void Throws_proper_exception_on_invalid_method_call_in_where()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o where id())-[Lines.Product]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("where") && 
                                e.Message.Contains("id") && 
                                e.Message.Contains("Orders"));

                    e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders as o)-[Lines.Product where id()]->(Products as p)
                        ").ToArray());                

                    Assert.True(e.Message.Contains("where") && 
                                e.Message.Contains("id") && 
                                e.Message.Contains("Lines.Product"));
                }
            }
        }

        [Fact]
        public void Throws_proper_exception_on_malformed_where_expression()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"
                           match (Orders  as o where id() = 'orders/825-A')-[Lines[].Product where Lines[].Product]->(Products as p)
                        ").ToArray());
                    Assert.True(e.Message.Contains("operator") &&
                                e.Message.Contains("Lines[].Product") &&
                                e.Message.Contains("]"));
                }
            }
        }
    }
}
