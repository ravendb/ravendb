using System.Linq;
using FastTests;
using FastTests.Graph;
using FastTests.Server.JavaScript;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

// ReSharper disable InconsistentNaming

namespace SlowTests.Issues
{
    public class RavenDB_12263 : RavenTestBase
    {
        public RavenDB_12263(ITestOutputHelper output) : base(output)
        {
        }

        private void CreateData(IDocumentStore store)
        {
          
            using (var session = store.OpenSession())
            {
                session.Store(new Dog
                {
                    Id = "dogs/1",
                    Likes = new []{"dogs/2", "dogs/3"}
                });
                session.Store(new Dog
                {
                    Id = "dogs/2",
                    Likes = new []{"dogs/4"}
                });
                session.Store(new Dog
                {
                    Id = "dogs/4",
                    Likes = new []{"dogs/6"}
                });
                session.Store(new Dog
                {
                    Id = "dogs/6",
                    Likes = new []{"dogs/8"}
                });
                session.Store(new Dog
                {
                    Id = "dogs/3",
                    Likes = new []{"dogs/5"}
                });                
                session.Store(new Dog
                {
                    Id = "dogs/5",
                    Likes = new []{"dogs/7"}
                });                
                session.Store(new Dog
                {
                    Id = "dogs/7",
                    Likes = new []{"dogs/8"}
                });                
                session.Store(new Dog
                {
                    Id = "dogs/8",
                });   
                session.SaveChanges();
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void All_paths_in_recursive_graph_queries_with_fixed_destination_should_return_proper_paths(string jsEngineType)
        {
            using(var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match(Dogs as Buddy where id() = 'dogs/1') 
                            -recursive as RecursiveLikes (all) 
                                { [Likes as PathElement] -> 
                                    (Dogs as TravelStep)
                                } -[Likes] -> 
                                    (Dogs as TraversalDestination where id() = 'dogs/8')
                        select 
                        {
                            IdOfTraversalStart : id(Buddy), 
                            LikesPath : RecursiveLikes.map(x => x.PathElement).join('>>'), 
                            IdOfTraversalEnd : id(TraversalDestination)
                        }
                    ").ToList();
                    
                    var likesPaths = results.Select(x => x["LikesPath"].Value<string>().Split(">>")).ToArray();
                    Assert.Equal(2, likesPaths.Length);
                    Assert.Equal(likesPaths[0],new []{"dogs/3", "dogs/5", "dogs/7" });
                    Assert.Equal(likesPaths[1],new []{"dogs/2", "dogs/4", "dogs/6" });
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void All_paths_in_recursive_graph_queries_without_fixed_destination_should_return_proper_paths(string jsEngineType)
        {
            using(var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        match(Dogs as Buddy where id() = 'dogs/1') 
                            -recursive as RecursiveLikes (all) 
                                { [Likes as PathElement] -> 
                                    (Dogs as TravelStep)
                                }
                        select 
                        {
                            IdOfTraversalStart : id(Buddy), 
                            LikesPath : RecursiveLikes.map(x => x.PathElement).join('>>')
                        }
                    ").ToList();
                    
                    var likesPaths = results.Select(x => x["LikesPath"].Value<string>()).ToArray();

                    Assert.Equal(8, likesPaths.Length);

                    Assert.Contains("dogs/2>>dogs/4>>dogs/6>>dogs/8", likesPaths);
                    Assert.Contains("dogs/2>>dogs/4>>dogs/6", likesPaths);
                    Assert.Contains("dogs/2>>dogs/4", likesPaths);
                    Assert.Contains("dogs/2", likesPaths);

                    Assert.Contains("dogs/3>>dogs/5>>dogs/7>>dogs/8", likesPaths);
                    Assert.Contains("dogs/3>>dogs/5>>dogs/7", likesPaths);
                    Assert.Contains("dogs/3>>dogs/5", likesPaths);
                    Assert.Contains("dogs/3", likesPaths);
                }
            }
        }
    }
}
