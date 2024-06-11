using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config;
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

        private readonly int _readTimeout = 500;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(1);

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [InlineData(null)]
        [InlineData(HttpProtocols.Http1)]
        [InlineData(HttpProtocols.Http2)]
        public async Task BulkInsertWithDelay(HttpProtocols? httpProtocols)
        {
            Version httpVersion;
            switch (httpProtocols)
            {
                case null:
                    httpVersion = null;
                    break;
                case HttpProtocols.Http1:
                    httpVersion = new Version(1, 1);
                    break;
                case HttpProtocols.Http2:
                    httpVersion = new Version(2, 0);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(httpProtocols));
            }

            DocumentConventions serverConventions = httpVersion != null
                ? new DocumentConventions { HttpVersion = httpVersion, HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact }
                : null;

            Dictionary<string, string> customSettings = httpVersion != null
                ? new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Http.Protocols), httpProtocols.ToString() } }
                : null;

            var server = GetNewServer(new ServerCreationOptions
            {
                Conventions = serverConventions,
                CustomSettings = customSettings
            });

            using (var store = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDocumentStore = s =>
                {
                    s.Conventions.HttpVersion = httpVersion;
                    s.Conventions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
                }
            }))
            {
                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
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

        [RavenFact(RavenTestCategory.BulkInsert | RavenTestCategory.Sharding)]
        public async Task BulkInsertWithDelaySharded()
        {
            DoNotReuseServer();
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);
                orchestrator.ForTestingPurposesOnly().BulkInsertStreamWriteTimeout = _readTimeout;
                var bulkInsertOptions = new BulkInsertOptions();
                bulkInsertOptions.ForTestingPurposesOnly().OverrideHeartbeatCheckInterval = _readTimeout;

                var bulk = store.BulkInsert(bulkInsertOptions);

                await Task.Delay(_delay);
                bulk.Store(new User { Name = "Daniel" }, "users/1");
                bulk.Store(new User { Name = "Yael" }, "users/2");

                await Task.Delay(_delay);
                bulk.Store(new User { Name = "Ido" }, "users/3");
                await Task.Delay(_delay);
                bulk.Dispose();

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

        [RavenFact(RavenTestCategory.BulkInsert | RavenTestCategory.Sharding)]
        public async Task StartStoreInTheMiddleOfAnHeartbeatSharded()
        {
            DoNotReuseServer();
            using (var store = Sharding.GetDocumentStore())
            {
                var mre = new ManualResetEvent(false);
                var orchestrator = Sharding.GetOrchestrator(store.Database);
                orchestrator.ForTestingPurposesOnly().BulkInsertStreamWriteTimeout = _readTimeout;
                var bulkInsertOptions = new BulkInsertOptions();
                bulkInsertOptions.ForTestingPurposesOnly().OverrideHeartbeatCheckInterval = _readTimeout;

                using (var bulk = store.BulkInsert(bulkInsertOptions))
                {
                    bulkInsertOptions.ForTestingPurposesOnly().OnSendHeartBeat_DoBulkStore = () =>
                    {
                        mre.Set();
                    };

                    Assert.True(mre.WaitOne(_delay * 3));

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
