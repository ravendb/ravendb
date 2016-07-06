using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3082 : RavenTestBase
    {
        [Fact]
        public async Task StronglyTypedDataSubscriptions()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
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

                var criteria = new SubscriptionCriteria<PersonWithAddress>
                {
                    FilterJavaScript = "return this.Name == 'James' && this.Address.ZipCode != 54321"
                };
                

                var id = await store.AsyncSubscriptions.CreateAsync(criteria);

                var subscription = await store.AsyncSubscriptions.OpenAsync<PersonWithAddress>(id, new SubscriptionConnectionOptions());

                var users = new BlockingCollection<PersonWithAddress>();

                subscription.Subscribe(users.Add);

                PersonWithAddress userToTake;
                for (var i = 0; i < 5; i++)
                {
                    Assert.True(users.TryTake(out userToTake, 1000));
                    Assert.Equal("James", userToTake.Name);
                    Assert.Equal(12345, userToTake.Address.ZipCode);
                }

                Assert.False(users.TryTake(out userToTake, 50));


            }
        }
    }
}