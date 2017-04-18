using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs.MapRedue
{
    public class LetInReduceFunction : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class IndexWithLetInReduceFunction : AbstractIndexCreationTask<User, IndexWithLetInReduceFunction.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public IndexWithLetInReduceFunction()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.Name
                               };

                Reduce = results => from result in results
                                    group result by result.Id
                                        into g
                                    let dummy = g.FirstOrDefault(x => x.Name != null)
                                    select new
                                    {
                                        Id = g.Key,
                                        Name = dummy.Name
                                    };
            }
        }

        [Fact]
        public void Can_perform_index_with_let_in_reduce_function()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/ayende", Name = "Ayende Rahien" });
                    session.Store(new User { Id = "users/dlang", Name = "Daniel Lang" });
                    session.SaveChanges();
                }

                new IndexWithLetInReduceFunction().Execute(store);

                WaitForIndexing(store);

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
