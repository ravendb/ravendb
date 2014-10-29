// -----------------------------------------------------------------------
//  <copyright file="Andrew.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.CodeDom;
using System.Linq;
using System.Threading;
using System.Web.SessionState;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Bugs.LiveProjections.Entities;
using Raven.Tests.Common;

using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Raven.Tests.Bugs
{
    public class Andrew :  RavenTest
    {
        public class User { }
        public class Car { }

        public class MyIndex : AbstractIndexCreationTask<User>
        {
            public MyIndex()
            {
                Map = users =>
                    from user in users
                    select new {A = LoadDocument<Car>("cars/1"), B = LoadDocument<Car>("cars/2"), ForceIndexRow = 1};
            }
        }

        [Fact]
        public void FunkyIndex()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                new MyIndex().Execute(store);

                WaitForIndexing(store);

                QueryResult firstQueryResult = store.DatabaseCommands.Query("MyIndex", new IndexQuery(), null);

                Assert.Equal(1, firstQueryResult.TotalResults);

                var cts = new CancellationTokenSource();


                var car1 = Task.Factory.StartNew(() =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        store.DatabaseCommands.Delete("cars/1", null);
                        Thread.Sleep(31);
                        store.DatabaseCommands.Put("cars/1", null, new RavenJObject(), new RavenJObject());
                    }
                });
                var car2 = Task.Factory.StartNew(() =>
                {
                    while (cts.IsCancellationRequested == false)
                    {
                        store.DatabaseCommands.Delete("cars/2", null);
                        Thread.Sleep(17);
                        store.DatabaseCommands.Put("cars/2", null, new RavenJObject(), new RavenJObject());
                    }
                });


                for (int i = 0; i < 100; i++)
                {
                    QueryResult queryResult = store.DatabaseCommands.Query("MyIndex", new IndexQuery(), null);

                    Assert.Equal(1, queryResult.TotalResults);
                }

                cts.Cancel();

                car1.Wait();
                car2.Wait();


                QueryResult finalQueryResult = store.DatabaseCommands.Query("MyIndex", new IndexQuery(), null);

                Assert.Equal(1, finalQueryResult.TotalResults);
            }
        }
    }
}