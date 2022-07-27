using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB14709 : RavenTestBase
    {
        public RavenDB14709(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(20);

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanQueryMetadataForSubscriptions(Options options)
        {
            using var store = GetDocumentStore(options);

            store.Subscriptions.Create(options: new SubscriptionCreationOptions
            {
                Name = "Subs1",
                Query = "from Users as u where u.'@metadata'.'r' == 1"
            });

            using (var session = store.OpenAsyncSession())
            {
                User entity = new User { Name = "abc" };
                await session.StoreAsync(entity);

                session.Advanced.GetMetadataFor(entity)["r"] = 1;

                await session.SaveChangesAsync();
            }


            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1")
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
            });
            var names = new List<string>();
            var subscriptionTask = subscription.Run(x =>
            {
                foreach (var item in x.Items)
                {
                    names.Add(item.Result.Name);
                }
            });

            var mre = new ManualResetEvent(false);

            subscription.AfterAcknowledgment += batch =>
            {
                mre.Set();
                return Task.CompletedTask;
            };

            Assert.True(mre.WaitOne(_reasonableWaitTime));
            await subscription.DisposeAsync();
            await subscriptionTask;
            Assert.NotEmpty(names);
        }
    }
}
