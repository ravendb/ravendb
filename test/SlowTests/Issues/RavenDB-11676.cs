using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Graph;
using FastTests.Server.JavaScript;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    //additional tests for edge-cases of RavenDB-11676 (mainstream cases are in the Graph API feature test suites)
    public class RavenDB_11676 : RavenTestBase
    {
        public RavenDB_11676(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_handle_string_array_as_edge_simple_select(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog { Name = "Arava" }; //dogs/1
                    var oscar = new Dog { Name = "Oscar" }; //dogs/2
                    var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                    session.Store(arava);
                    session.Store(oscar);
                    session.Store(pheobe);

                    arava.Likes = new[] { oscar.Id, pheobe.Id };

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    List<(string Dog, string Likes)> results = session.Advanced.RawQuery<JObject>(
                        @"match (Dogs as src)-[Likes as likes]->(Dogs as dest) 
                          select id(src) as dog,likes").ToList()
                        .Select(x => (x["dog"].Value<string>(), x["likes"].Value<string>())).ToList();

                    Assert.Equal(2, results.Count);
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/2-A");
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/3-A");
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_handle_string_array_as_edge_with_select_and_recursive_query(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog { Name = "Arava" }; //dogs/1
                    var oscar = new Dog { Name = "Oscar" }; //dogs/2
                    var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                    session.Store(arava);
                    session.Store(oscar);
                    session.Store(pheobe);

                    arava.Likes = new[] { oscar.Id, pheobe.Id };

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    List<(string Dog, string Likes)> results = session.Advanced.RawQuery<JObject>(
                        @"match (Dogs as src)-recursive as r (all) { [Likes as likes]->(Dogs as dest) }
                          select id(src) as dog,r").ToList().Select(x => (x["dog"].Value<string>(), x["r"][0]["likes"].Value<string>())).ToList();

                    Assert.Equal(2, results.Count);
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/2-A");
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/3-A");
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Can_handle_string_array_as_edge_js_select(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog { Name = "Arava" }; //dogs/1
                    var oscar = new Dog { Name = "Oscar" }; //dogs/2
                    var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                    session.Store(arava);
                    session.Store(oscar);
                    session.Store(pheobe);

                    arava.Likes = new[] { oscar.Id, pheobe.Id };

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    List<(string Dog, string Likes)> results = session.Advanced.RawQuery<JObject>(
                            @"match (Dogs as src)-[Likes as likes]->(Dogs as dest) 
                          select { Dog: id(src), Likes: likes }").ToList()
                        .Select(x => (x["Dog"].Value<string>(), x["Likes"].Value<string>())).ToList();

                    Assert.Equal(2, results.Count);
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/2-A");
                    Assert.Contains(results, x => x.Dog == "dogs/1-A" && x.Likes == "dogs/3-A");
                }
            }
        }

    }
}
