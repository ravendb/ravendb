using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs.MapRedue
{
    public class LargeKeysInVoron : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string[] Aliases { get; set; }

            public User()
            {
                this.Aliases = new string[] { };
            }
        }

        private class LargeKeysInVoronFunction : AbstractIndexCreationTask<User, LargeKeysInVoronFunction.ReduceResult>
        {
            public class ReduceResult
            {
                public string Name { get; set; }
                public int Count { get; set; }
                public AliasReduceResult[] Aliases { get; set; }

                public class AliasReduceResult
                {
                    public string Name { get; set; }
                    public string Alias { get; set; }
                }
            }

            public LargeKeysInVoronFunction()
            {
                Map = users => from user in users
                               let aliases = from alias in user.Aliases
                                             select new
                                             {
                                                 user.Name,
                                                 Alias = alias
                                             }
                               from alias in aliases
                               group aliases by new
                               {
                                   Name = alias.Name,
                               } into g
                               select new
                               {
                                   Name = g.Key.Name,
                                   Count = g.Count(),
                                   Aliases = g
                               };

                Reduce = users => from user in users
                                  group user by new
                                  {
                                      Name = user.Name,
                                      Count = user.Count,
                                  } into g
                                  select new ReduceResult
                                  {
                                      // This is the bag used on the reduce.
                                      Name = g.Key.Name,
                                      Count = g.Key.Count,
                                      Aliases = g.First().Aliases
                                  };

                StoreAllFields(FieldStorage.Yes);
            }
        }



        [Fact]
        public void CanHandleLargeReduceKeys()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "name/ayende", Name = new string('A', 10000), Aliases = new[] { "alias1", "alias2" } });
                    session.Store(new User { Id = "name/ayende2", Name = new string('A', 10000), Aliases = new[] { "alias1", "alias3" } });
                    session.SaveChanges();
                }

                new LargeKeysInVoronFunction().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<LargeKeysInVoronFunction.ReduceResult, LargeKeysInVoronFunction>()
                                       .ToList();

                    Assert.Equal(1, query.Count());

                    var result = query.First();
                    Assert.Equal(2, result.Count);
                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var indexes = db.IndexStore.GetIndexes();
                var errorsCount = indexes.Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
