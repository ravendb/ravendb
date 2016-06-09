using System;
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
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new PersonWithAddress()
                        {
                            Name = "James",
                            Address = new Address()
                            {
                                ZipCode = 12345
                            }
                        });

                        session.Store(new PersonWithAddress()
                        {
                            Name = "James",
                            Address = new Address()
                            {
                                ZipCode = 54321
                            }
                        });

                        session.Store(new PersonWithAddress()
                        {
                            Name = "David",
                            Address = new Address()
                            {
                                ZipCode = 12345
                            }
                        });

                        session.Store(new Person());
                    }

                    session.SaveChanges();
                }

                var criteria = new SubscriptionCriteria<PersonWithAddress>
                {
                    FilterJavaScript = "return this.Name == 'James' && this.Address.ZipCode != 54321"
                };
                

                var id = await store.AsyncSubscriptions.CreateAsync(criteria);

                var subscription = await store.AsyncSubscriptions.OpenAsync<PersonWithAddress>(id, new SubscriptionConnectionOptions());

                var users = new List<PersonWithAddress>();

                subscription.Subscribe(users.Add);

                Assert.True(SpinWait.SpinUntil(() => users.Count >= 10, TimeSpan.FromSeconds(60)));

                Assert.Equal(10, users.Count);

                foreach (var user in users)
                {
                    Assert.Equal("James", user.Name);
                    Assert.Equal(12345, user.Address.ZipCode);
                }
            }
        }
    }
}