using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace SlowTests.Bugs
{
    public class Andrew : RavenTestBase
    {
        public Andrew(ITestOutputHelper output) : base(output)
        {
        }

        private class User { }
        private class Car { }

        private class MyIndex : AbstractIndexCreationTask<User>
        {
            public MyIndex()
            {
                Map = users =>
                    from user in users
                    select new { A = LoadDocument<Car>("cars/1"), B = LoadDocument<Car>("cars/2"), ForceIndexRow = 1 };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task FunkyIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                await new MyIndex().ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var firstQueryResult = await store.Commands().QueryAsync(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

                Assert.Equal(1, firstQueryResult.TotalResults);

                var cts = new CancellationTokenSource();


                var car1 = Task.Factory.StartNew(() =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        store.Commands().Delete("cars/1", null);
                        Thread.Sleep(31);
                        store.Commands().Put("cars/1", null, new object());

                    }
                });
                var car2 = Task.Factory.StartNew(() =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        store.Commands().Delete("cars/2", null);
                        Thread.Sleep(17);
                        store.Commands().Put("cars/2", null, new object());
                    }
                });


                for (int i = 0; i < 100; i++)
                {
                    QueryResult queryResult = await store.Commands().QueryAsync(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

                    Assert.Equal(1, queryResult.TotalResults);
                }

                await cts.CancelAsync();

                await car1.WaitAsync(TimeSpan.FromMinutes(1));
                await car2.WaitAsync(TimeSpan.FromMinutes(1));

                QueryResult finalQueryResult = await store.Commands().QueryAsync(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

                Assert.Equal(1, finalQueryResult.TotalResults);
            }
        }
    }
}