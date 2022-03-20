using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.MapRedue
{
    public class LetInReduceFunction : RavenTestBase
    {
        public LetInReduceFunction(ITestOutputHelper output) : base(output)
        {
        }

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
        public async Task Can_perform_index_with_let_in_reduce_function()
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

                Indexes.WaitForIndexing(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
