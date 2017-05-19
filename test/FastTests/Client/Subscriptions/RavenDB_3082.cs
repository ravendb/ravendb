using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using FastTests.Server.Documents.Notifications;
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
                    Criteria = new SubscriptionCriteria<PersonWithAddress>
                    {
                        FilterJavaScript = "return this.Name == 'James' && this.Address.ZipCode != 54321"
                    }
                };


                var id = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.AsyncSubscriptions.Open<PersonWithAddress>(new SubscriptionConnectionOptions(id)))
                {
                    var users = new BlockingCollection<PersonWithAddress>();

                    subscription.Subscribe(users.Add);
                    await subscription.StartAsync();

                    PersonWithAddress userToTake;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(users.TryTake(out userToTake, 50000));
                        Assert.Equal("James", userToTake.Name);
                        Assert.Equal(12345, userToTake.Address.ZipCode);
                    }

                    Assert.False(users.TryTake(out userToTake, 50));


                }
            }
        }
    }
}