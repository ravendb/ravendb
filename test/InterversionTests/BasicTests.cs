using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class BasicTests : InterversionTestBase
    {
        public BasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Test()
        {
            var getStoreTask407 = GetDocumentStoreAsync("4.0.7");
            var getStoreTask408 = GetDocumentStoreAsync("4.0.8");

            await Task.WhenAll(getStoreTask407, getStoreTask408);

            AssertStore(await getStoreTask407);
            AssertStore(await getStoreTask408);
            AssertStore(GetDocumentStore());
        }

        [Theory]
        [InlineData("5.2.3")]
        [InlineData("5.3.0-nightly-20211107-0402")]
        public async Task SubscriptionTest(string version)
        {
            using (var store = await GetDocumentStoreAsync(version))
            {
                var id = store.Subscriptions.Create<User>();
                
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxDocsPerBatch = 2
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "user/1");
                        session.Store(new User(), "user/2");
                        session.Store(new User(), "user/3");
                        session.Store(new User(), "user/4");
                        session.Store(new User(), "user/5");
                        session.Store(new User(), "user/6");
                        session.SaveChanges();
                    }

                    var con1Docs = new List<string>();
                    
                    var t = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            con1Docs.Add(item.Id);
                        }
                    });

                    await AssertWaitForTrueAsync(() => Task.FromResult(con1Docs.Count == 6), 6000);
                }
            }
        }

        private static void AssertStore(IDocumentStore store)
        {
            using (store)
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c = session.Load<Company>("companies/1");
                    Assert.NotNull(c);
                    Assert.Equal("HR", c.Name);
                }
            }
        }
    }
}
