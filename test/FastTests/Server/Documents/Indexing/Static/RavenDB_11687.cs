using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class RavenDB_11687 : RavenTestBase
    {
        [Fact]
        public void CanIndexDictionaryDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new IndexReturningDictionary_MethodSyntax().Execute(store);
                new IndexReturningDictionary_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "arek",
                        Age = 32
                    });

                    session.Store(new User()
                    {
                        Name = "joe",
                        Age = 33
                    });

                    session.SaveChanges();

                    var users = session.Query<User, IndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(2, users.Count);

                    users = session.Query<User, IndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age == 32).ToList();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("arek", users[0].Name);

                    users = session.Query<User, IndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(2, users.Count);

                    users = session.Query<User, IndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age == 32).ToList();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("arek", users[0].Name);
                }
            }
        }

        [Fact]
        public void CanMapReduceIndexDictionaryDirectly()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduceIndexReturningDictionary_MethodSyntax().Execute(store);
                new MapReduceIndexReturningDictionary_QuerySyntax().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "arek",
                        Age = 32
                    });

                    session.Store(new User()
                    {
                        Name = "joe",
                        Age = 32
                    });

                    session.SaveChanges();

                    var results = session.Query<MapReduceIndexReturningDictionary_MethodSyntax.Result, MapReduceIndexReturningDictionary_MethodSyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal(32, results[0].Age);

                    results = session.Query<MapReduceIndexReturningDictionary_MethodSyntax.Result, MapReduceIndexReturningDictionary_QuerySyntax>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, results.Count);

                    Assert.Equal(2, results[0].Count);
                    Assert.Equal(32, results[0].Age);
                }
            }
        }

        private class IndexReturningDictionary_MethodSyntax : AbstractIndexCreationTask<User>
        {
            public IndexReturningDictionary_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Age},
                    {"Name", x.Name}
                });
            }
        }

        private class MapReduceIndexReturningDictionary_MethodSyntax : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public int Age { get; set; }
                public int Count { get; set; }
            }

            public MapReduceIndexReturningDictionary_MethodSyntax()
            {
                Map = users => users.Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Age},
                    {"Count", 1}
                });

                Reduce = results => results.GroupBy(x => x.Age).Select(x => new Dictionary<string, object>()
                {
                    {"Age", x.Key},
                    {"Count", x.Sum(y => y.Count)}
                });
            }
        }

        private class IndexReturningDictionary_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from user in docs.Users select new Dictionary <string, object >() { {""Age"", user.Age}, {""Name"", user.Name} }"
                    }
                };
            }
        }

        private class MapReduceIndexReturningDictionary_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = new HashSet<string>
                    {
                        @"from user in docs.Users select new Dictionary<string, object>() { {""Age"", user.Age}, {""Count"", 1} }"
                    },
                    Reduce = @"from result in results group result by result.Age into g select new Dictionary<string, object>() { {""Age"", g.Key}, {""Count"", g.Sum(x => x.Count)} }"
                };
            }
        }
    }
}
