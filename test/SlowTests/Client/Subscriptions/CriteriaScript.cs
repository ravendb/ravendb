using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class CriteriaScript : SubscriptionTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(30);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task BasicCriteriaTest(bool useSsl)
        {
            string dbName = GetDatabaseName();
            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var serverCertPath = SetupServerAuthentication();
                adminCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    [dbName] = DatabaseAccess.ReadWrite
                });
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    await CreateDocuments(store, 1);

                    var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                    await CreateDocuments(store, 5);

                    var subscriptionCreationParams = new SubscriptionCreationOptions()
                    {
                        Query = "from Things where Name = 'ThingNo3'",
                        ChangeVector = lastChangeVector
                    };
                    var subsId = subscriptionManager.Create(subscriptionCreationParams);
                    using (var subscription = subscriptionManager.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        var list = new BlockingCollection<Thing>();
                        GC.KeepAlive(subscription.Run(x =>
                        {
                            foreach (var item in x.Items)
                            {
                                list.Add(item.Result);
                            }
                        }));

                        Thing thing;
                        Assert.True(list.TryTake(out thing, _reasonableWaitTime));
                        Assert.Equal("ThingNo3", thing.Name);
                        Assert.False(list.TryTake(out thing, 50));
                    }
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CriteriaScriptWithTransformation(bool useSsl)
        {
            string dbName = GetDatabaseName();
            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var serverCertPath = SetupServerAuthentication();
                adminCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    [dbName] = DatabaseAccess.ReadWrite,
                });
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    await CreateDocuments(store, 1);

                    var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                    await CreateDocuments(store, 6);

                    var subscriptionCreationParams = new SubscriptionCreationOptions()
                    {
                        Query = @"
declare function project(d) {
    var namSuffix = parseInt(d.Name.replace('ThingNo', ''));  
    if (namSuffix <= 2){
        return false;
    }
    else if (namSuffix == 3){
        return null;
    }
    else if (namSuffix == 4){
        return d;
    }
    return {Name: 'foo', OtherDoc:load('things/6-A')}
}
from Things as d
select project(d)
",
                        ChangeVector = lastChangeVector
                    };

                    var subsId = subscriptionManager.Create(subscriptionCreationParams);
                    using (var subscription = subscriptionManager.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {
                        using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            var list = new BlockingCollection<Thing>();

                            GC.KeepAlive(subscription.Run(x =>
                            {
                                foreach (var item in x.Items)
                                {
                                    list.Add(item.Result);
                                }
                            }));

                            Thing thing;

                            Assert.True(list.TryTake(out thing, _reasonableWaitTime));
                            Assert.Equal("ThingNo4", thing.Name);
                            Assert.True(list.TryTake(out thing, _reasonableWaitTime));
                            Assert.Equal("foo", thing.Name);
                            Assert.Equal("ThingNo4", thing.OtherDoc.Name);
                            Assert.False(list.TryTake(out thing, 50));
                        }
                    }
                }
            }
        }
    }
}
