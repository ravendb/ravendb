using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3484 : RavenTestBase
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void AllClientsWith_WaitForFree_StrategyShouldGetAccessToSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var processed = new ManualResetEventSlim[numberOfClients];
                for (int i = 0; i < numberOfClients; i++)
                {
                    processed[i] = new ManualResetEventSlim();
                }
                int? processedClient = null;
                var done = new ManualResetEventSlim();

                for (int i = 0; i < numberOfClients; i++)
                {
                    var clientNumber = i;

                    subscriptions[clientNumber] = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    subscriptions[clientNumber].AfterAcknowledgment += x =>
                    {
                        processed[clientNumber].Set();
                        return Task.CompletedTask;
                    };

                    subscriptions[clientNumber].Run(x =>
                    {
                        processedClient = clientNumber;
                        done.Set();
                    });
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.SaveChanges();
                    }

                    Assert.True(done.Wait(waitForDocTimeout));

                    Assert.True(processed[processedClient.Value].Wait(waitForDocTimeout));

                    subscriptions[processedClient.Value].Dispose();

                    processedClient = null;
                    done.Reset();
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    Assert.True(processed[i].IsSet);
                }
            }
        }
    }
}
