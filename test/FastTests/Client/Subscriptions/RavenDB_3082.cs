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
                        Filter = p => p.Name == "James" && p.Address.ZipCode != 54321,
                        Project = p => new
                        {
                            Name = p.Name,
                            ZipCode = p.Address.ZipCode
                        }
                    }
                );

                using (
                    var subscription =
                        store.Subscriptions.Open<PersonWithZipcode>(new SubscriptionConnectionOptions(id)))
                {
                    var users = new BlockingCollection<PersonWithZipcode>();

                    GC.KeepAlive(subscription.Run(address =>
                    {
                        foreach (var item in address.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    PersonWithZipcode userToTake;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(users.TryTake(out userToTake, 500000));
                        Assert.Equal("James", userToTake.Name);
                        Assert.Equal(12345, userToTake.ZipCode);
                    }

                    Assert.False(users.TryTake(out userToTake, 50));


                }
            }
        }
    }
}
