using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_3082 : RavenTestBase
    {

        public class PersonWithZipcode
        {
            public string Name { get; set; }
            public int ZipCode { get; set; }
        }

        [Fact]
        public async Task StronglyTypedDataSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new PersonWithAddress()
                        {
                            Name = "James",
                            Address = new Address()
                            {
                                ZipCode = 12345
                            }
                        });

                        await session.StoreAsync(new PersonWithAddress()
                        {
                            Name = "James",
                            Address = new Address()
                            {
                                ZipCode = 54321
                            }
                        });

                        await session.StoreAsync(new PersonWithAddress()
                        {
                            Name = "David",
                            Address = new Address()
                            {
                                ZipCode = 12345
                            }
                        });

                        await session.StoreAsync(new Person());
                    }

                    await session.SaveChangesAsync();
                }

                var id = store.Subscriptions.Create(
                    new SubscriptionCreationOptions<PersonWithAddress>
                    {
                        Filter = p => p.Name == "James" && p.Address.ZipCode != 54321
                    }
                );

                using (var subscription =
                        store.Subscriptions.GetSubscriptionWorker<PersonWithAddress>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<PersonWithAddress>();

                    GC.KeepAlive(subscription.Run(address =>
                    {
                        foreach (var item in address.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    PersonWithAddress userToTake;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(users.TryTake(out userToTake, 500000));
                        Assert.Equal("James", userToTake.Name);
                        Assert.Equal(12345, userToTake.Address.ZipCode);
                    }

                    Assert.False(users.TryTake(out userToTake, 50));


                }
            }
        }
    }
}
