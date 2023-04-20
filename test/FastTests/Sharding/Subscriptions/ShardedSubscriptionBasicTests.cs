using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Json.Sync;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Subscriptions
{
    public class ShardedSubscriptionBasicTests : RavenTestBase
    {
        public ShardedSubscriptionBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public async Task CanRunSubscription()
        {
            using (var store = Sharding.GetDocumentStore(/*shards: new[] { new DatabaseTopology(), new DatabaseTopology() }*/))
            {
                var id = store.Subscriptions.Create<User>();
                var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, 5);
                Assert.Equal(1, subscriptions.Count);

                var names = new List<string>
                {
                    "EGOR", "egor", "EGR"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = names[0] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[1] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[2] }, Guid.NewGuid().ToString());
                    session.SaveChanges();
                }

                var mre = new AsyncManualResetEvent();
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    var c = 0;
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            names.Remove(item.Result.Name);
                            if (++c == 3)
                            {
                                mre.Set();
                            }
                        }
                    });

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                    Assert.Empty(names);
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
        public async Task CanTrySubscription()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var names = new List<string>
                {
                    "EGOR", "egor", "EGR"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = names[0] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[1] }, Guid.NewGuid().ToString());
                    session.Store(new User() { Name = names[2] }, Guid.NewGuid().ToString());
                    session.SaveChanges();
                }

                using var client = new HttpClient();
                var url = $"{store.Urls.First()}/databases/{Uri.EscapeDataString(store.Database)}/subscriptions/try?pageSize=10";
                var tryout = new SubscriptionTryout() { Query = "from Users" };
                var serializeObject = JsonConvert.SerializeObject(tryout);
                var data = new StringContent(serializeObject, Encoding.UTF8, "application/json");
                var rawVersionRespond = (await client.PostAsync(url, data)).Content.ReadAsStringAsync().Result;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(rawVersionRespond, "test");
                    var res = JsonDeserializationClient.GetDocumentsResult(bjro);
                    foreach (var doc in res.Results)
                    {
                        if (doc is BlittableJsonReaderObject blittable)
                        {
                            if (blittable.TryGetMember(nameof(User.Name), out var obj) && obj is LazyStringValue id)
                            {
                                names.Remove(id);
                            }
                        }

                    }

                    Assert.Empty(names);
                }
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-16279");
        }
    }
}
