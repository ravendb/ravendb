using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Sparrow;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12127 : RavenTestBase
    {
        private class Dog
        {
#pragma warning disable 414
            public string Name;
#pragma warning restore 414
            public string Owner;
        }

        private class Person
        {
#pragma warning disable 414
            public string Name;
#pragma warning restore 414
        }

        [Fact]
        public async Task CanUseSubscriptionWithTcpProtocol40()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Owner = "people/1"
                    });

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = @"from Dogs"
                });

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Dog>(id))
                {
                    sub.SubscriptionTcpVersion = 40;

                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));
                    await sub.DisposeAsync();
                    await r;// no error
                }
            }
        }

        [Fact]
        public async Task CannotUseSubscriptionWithIncludesWhenProtocolDoesNotSupportIt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arava"
                    }, "people/1");
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Owner = "people/1"
                    });
                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = @"from Dogs include Owner"
                });

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Dog>(id))
                {
                    sub.SubscriptionTcpVersion = 40;

                    var r = sub.Run(batch => { });
                    var e = await Assert.ThrowsAsync<SubscriptionInvalidStateException>(() => r);
                    Assert.Contains("requires the protocol to support Includes", e.Message);

                    await sub.DisposeAsync(); 
                }
            }
        }
    }
}
