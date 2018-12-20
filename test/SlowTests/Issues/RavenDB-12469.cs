using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12469 : RavenTestBase
    {
        private class User
        {
            public string UserId { get; set; }
            public string[] Friends { get;set; }
        }
        private void CreateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    UserId = "users/1",
                    Friends = new []{ "users/2", "users/4" }
                },"users/1");
                session.Store(new User
                {
                    UserId = "users/2",
                    Friends = new []{ "users/3" }
                },"users/2");
                session.Store(new User
                {
                    UserId = "users/3",
                    Friends = Array.Empty<string>()
                },"users/3");
                session.Store(new User
                {
                    UserId = "users/4",
                    Friends = Array.Empty<string>()
                },"users/4");
                session.SaveChanges();
            }
        }
        [Fact]
        public void Should_handle_duplicate_implicit_aliases()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var queryResult = session.Advanced.RawQuery<JObject>(@"
                        with { from Users where id() =  'users/1'} as start
                        match    (start)-[Friends]->(Users as f1)-[Friends]->(Users as f2)
                        and not  (start)-[Friends]->(f2)").ToList();
                    Assert.Equal(1,queryResult.Count);
                    var f2UserId = queryResult[0]["f2"]["UserId"].Value<string>();
                    Assert.Equal("users/3",f2UserId);
                }
            }
        }
    }
}
