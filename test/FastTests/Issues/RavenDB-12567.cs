using System.Linq;
using FastTests.Graph;
using Newtonsoft.Json.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12567 : RavenTestBase
    {
        [Fact]
        public void Recursive_queries_should_handle_self_cycles_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new [] { "dogs/1" }
                    },"dogs/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d1)- recursive as r (all) { [Likes as path]->(Dogs as d2) }
                        select {
                            Start: id(d1), 
                            Path: r.map(x => x.path).join('->')
                        }
                    ").ToList();

                    Assert.True(queryResults.Count == 1);

                    Assert.Equal("dogs/1",queryResults[0]["Start"].Value<string>());
                    Assert.Equal("dogs/1",queryResults[0]["Path"].Value<string>());
                }
            }
        }

        [Fact]
        public void Recursive_queries_with_self_cycles_and_regular_cycles_should_properly_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new [] { "dogs/1", "dogs/2" }
                    },"dogs/1");

                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new [] { "dogs/1" }
                    },"dogs/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Advanced.RawQuery<JObject>(@"
                        match (Dogs as d1)- recursive as r (all) { [Likes as path]->(Dogs as d2) }
                        select {
                            Start: id(d1), 
                            Path: r.map(x => x.path).join('->')
                        }
                    ").ToList();

                    Assert.Equal(queryResults.Count, 7);
                    //TODO : finish assertion in this test (after verifying that the code is good)
                }
            }
        }
    }
}
