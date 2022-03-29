// -----------------------------------------------------------------------
//  <copyright file="Andrew.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Task = System.Threading.Tasks.Task;
using Xunit.Abstractions;

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

        [Fact]
        public void FunkyIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                new MyIndex().Execute(store);

                Indexes.WaitForIndexing(store);

                var firstQueryResult = store.Commands().Query(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

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
                    QueryResult queryResult = store.Commands().Query(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

                    Assert.Equal(1, queryResult.TotalResults);
                }

                cts.Cancel();

                Assert.True(car1.Wait(TimeSpan.FromMinutes(1)));
                Assert.True(car2.Wait(TimeSpan.FromMinutes(1)));

                QueryResult finalQueryResult = store.Commands().Query(new IndexQuery { Query = "FROM INDEX 'MyIndex'" });

                Assert.Equal(1, finalQueryResult.TotalResults);
            }
        }
    }
}
