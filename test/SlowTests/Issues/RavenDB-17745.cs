using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using SlowTests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17745 : RavenTestBase
    {
        public RavenDB_17745(ITestOutputHelper output) : base(output)
        {
        }

        private readonly int _readTimeout = 5000;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(10);

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task BulkInsertWithDelay()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                db.ForTestingPurposesOnly().BulkInsertStreamReadTimeout = _readTimeout;
                var bulkInsertOptions = new BulkInsertOptions();
                bulkInsertOptions.ForTestingPurposesOnly().OverrideHeartbeatCheckInterval = _readTimeout;

                await using (var bulk = store.BulkInsert(bulkInsertOptions))
                {
                    await Task.Delay(_delay);
                    await bulk.StoreAsync(new User { Name = "Daniel" }, "users/1");
                    await bulk.StoreAsync(new User { Name = "Yael" }, "users/2");

                    await Task.Delay(_delay);
                    await bulk.StoreAsync(new User { Name = "Ido" }, "users/3");
                    await Task.Delay(_delay);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);

                    user = session.Load<User>("users/2");
                    Assert.NotNull(user);
                    Assert.Equal("Yael", user.Name);

                    user = session.Load<User>("users/3");
                    Assert.NotNull(user);
                    Assert.Equal("Ido", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task StartStoreInTheMiddleOfAnHeartbeat()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var mre = new AsyncManualResetEvent();
                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                db.ForTestingPurposesOnly().BulkInsertStreamReadTimeout = _readTimeout;
                var bulkInsertOptions = new BulkInsertOptions();
                bulkInsertOptions.ForTestingPurposesOnly().OverrideHeartbeatCheckInterval = _readTimeout;

                await using (var bulk = store.BulkInsert(bulkInsertOptions))
                {
                    bulkInsertOptions.ForTestingPurposesOnly().OnSendHeartBeat_DoBulkStore = () =>
                    {
                        mre.Set();
                    };

                    Assert.True(await mre.WaitAsync(_delay * 3));

                    await bulk.StoreAsync(new User { Name = "Daniel" }, "users/1");
                }


                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);
                }
            }
        }
    }
}
