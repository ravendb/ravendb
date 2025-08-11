using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Collections;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24640 : RavenTestBase
    {
        public RavenDB_24640(ITestOutputHelper output) : base(output)
        {
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }


        [RavenFact(RavenTestCategory.Conventions)]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore(new Options()
                   {
                       ModifyDocumentStore = s =>
                       {
                           s.Conventions.PreserveDocumentPropertiesNotFoundOnModel = true;
                           s.Conventions.IdentityPartsSeparator = '-';
                           s.Conventions.MaxNumberOfRequestsPerSession = int.MaxValue;
                           s.Conventions.UseOptimisticConcurrency = true;
                       }
                   }))
            {
                var mre = new AsyncManualResetEvent();
                var testDoc = new TestDocument { Name = "Foo", Id = "TestDocuments-1-A" };


                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(testDoc);
                    await session.SaveChangesAsync();
                }

                var options = new SubscriptionCreationOptions<TestDocument> { Name = "Test" };
                await store.Subscriptions.CreateAsync(options, store.Database);
                var worker = store.Subscriptions.GetSubscriptionWorker<TestDocument>(
                    new SubscriptionWorkerOptions(options.Name) { Strategy = SubscriptionOpeningStrategy.TakeOver }, store.Database);

                var s = new ConcurrentSet<string>();

                worker.AfterAcknowledgment += async batch =>
                {
                    foreach (var i in batch.Items)
                    {
                        s.Add(i.Result.Name);
                    }
                };
                _ = worker
                    .Run(async batch =>
                    {
                        using (var session = batch.OpenAsyncSession())
                        {
                            await session.SaveChangesAsync();
                        }

                        if (mre.IsSet == false)
                        {
                            mre.Set();
                        }

                    });
                Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));


                using (var commands = store.Commands())
                {
                    testDoc.Name = "Documents";
                    var str = JsonSerializer.Serialize<TestDocument>(testDoc);
                    dynamic json = JObject.Parse(str);
                    json.Foo = "EGOR";
                    await commands.PutAsync("TestDocuments-1-A", null, json,
                        new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "TestDocuments" } });
                }

                await WaitForValueAsync(() => s.Count, 2, 60_000);

                using (var commands = store.Commands())
                {
                    dynamic doc = await commands.GetAsync("TestDocuments-1-A");
                    Assert.Equal("Documents", doc.Name);
                    Assert.Equal("EGOR", doc.Foo); // This is failing because Foo is missing
                }
            }
        }
    }
}
