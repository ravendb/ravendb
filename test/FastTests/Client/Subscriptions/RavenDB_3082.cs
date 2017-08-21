using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3082 : RavenTestBase
    {
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

                var subscriptionCreationParams = new SubscriptionCreationOptions<PersonWithAddress>()
                {
                    Criteria = new SubscriptionCriteria<PersonWithAddress>(p => p.Name == "James" && p.Address.ZipCode != 54321)
                };


                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.Subscriptions.Open<PersonWithAddress>(new SubscriptionConnectionOptions(id)))
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
