using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class CriteriaScript : SubscriptionTestBase
    {
        [Theory]
        [InlineData(false)]
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

            using (var store = GetDocumentStore(adminCertificate: adminCertificate, userCertificate: clientCertificate, modifyName: s => dbName))
            {
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    await CreateDocuments(store, 1);

                    var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                    await CreateDocuments(store, 5);

                    var subscriptionCreationParams = new SubscriptionCreationOptions()
                    {
                        Criteria = new SubscriptionCriteria("Things")
                        {
                            Script = " return this.Name == 'ThingNo3'"
                        },
                        ChangeVector = lastChangeVector
                    };
                    var subsId = subscriptionManager.Create(subscriptionCreationParams);
                    using (var subscription = subscriptionManager.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
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
                        Assert.True(list.TryTake(out thing, 5000));
                        Assert.Equal("ThingNo3", thing.Name);
                        Assert.False(list.TryTake(out thing, 50));
                    }
                }
            }
        }

        [Theory]
        [InlineData(false)]
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

            using (var store = GetDocumentStore(adminCertificate: adminCertificate, userCertificate: clientCertificate, modifyName: s => dbName))
            {
                using (var subscriptionManager = new DocumentSubscriptions(store))
                {
                    await CreateDocuments(store, 1);

                    var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                    await CreateDocuments(store, 6);

                    var subscriptionCreationParams = new SubscriptionCreationOptions()
                    {
                        Criteria = new SubscriptionCriteria("Things")
                        {
                            Script =
                                @"var namSuffix = parseInt(this.Name.replace('ThingNo', ''));  
                    if (namSuffix <= 2){
                        return false;
                    }
                    else if (namSuffix == 3){
                        return null;
                    }
                    else if (namSuffix == 4){
                    return this;
                    }
                    return {Name: 'foo', OtherDoc:load('things/6-A')}",
                        },
                        ChangeVector = lastChangeVector
                    };

                    var subsId = subscriptionManager.Create(subscriptionCreationParams);
                    using (var subscription = subscriptionManager.Open<BlittableJsonReaderObject>(new SubscriptionConnectionOptions(subsId)))
                    {
                        using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            var list = new BlockingCollection<BlittableJsonReaderObject>();

                            GC.KeepAlive(subscription.Run(x =>
                            {
                                foreach (var item in x.Items)
                                {
                                    list.Add(context.ReadObject(item.Result, "test"));
                                }
                            }));

                            BlittableJsonReaderObject thing;

                            Assert.True(list.TryTake(out thing, 5000));
                            dynamic dynamicThing = new DynamicBlittableJson(thing);
                            Assert.Equal("ThingNo4", dynamicThing.Name);


                            Assert.True(list.TryTake(out thing, 5000));
                            dynamicThing = new DynamicBlittableJson(thing);
                            Assert.Equal("foo", dynamicThing.Name);
                            Assert.Equal("ThingNo4", dynamicThing.OtherDoc.Name);

                            Assert.False(list.TryTake(out thing, 50));
                        }
                    }
                }
            }
        }
    }
}
