using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_8404:RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);
        
        [Fact]
        public async Task SubscriptionsRQLSupportStartsWith()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u Where startsWith(u.Name,'Th')"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Thor"
                    });
                    session.Store(new User()
                    {
                        Name = "The emperror"
                    });
                    session.Store(new User()
                    {
                        Name = "Thee"
                    });
                    session.Store(new User{Name = "you"});
                    session.SaveChanges();
                }

                var subscription = store.Subscriptions.Open<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    amre.Set();
                });
                
                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }

        [Fact]
        public async Task SubscriptionsRQLSupportEndsWith()
        {            
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u Where endsWith(u.Name,'nd')"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Thor the second"
                    });
                    session.Store(new User()
                    {
                        Name = "The emperror"
                    });
                    session.Store(new User()
                    {
                        Name = "Thee the second"
                    });
                    session.Store(new User { Name = "you the second" });
                    session.SaveChanges();
                }

                var subscription = store.Subscriptions.Open<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    amre.Set();
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }


        [Fact]
        public async Task SubscriptionsRQLSupportRegex()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u Where regex(u.Name,'^Th')"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Thor the second"
                    });
                    session.Store(new User()
                    {
                        Name = "The emperror"
                    });
                    session.Store(new User()
                    {
                        Name = "Thee the second"
                    });
                    session.Store(new User { Name = "you the second" });
                    session.SaveChanges();
                }

                var subscription = store.Subscriptions.Open<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    amre.Set();
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }

        public class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task SubscriptionsRQLSupportExists()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u Where exists(u.AddressId)"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Raven.Tests.Core.Utils.Entities.User()
                    {
                        Name = "Thor the second"
                    });
                    session.Store(new User()
                    {
                        Name = "The emperror"
                    });
                    session.Store(new User()
                    {
                        Name = "Thee the second"
                    });
                    session.Store(new User { Name = "you the second" });
                    session.SaveChanges();
                }

                var subscription = store.Subscriptions.Open<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    amre.Set();
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(1, users.Count);
            }
        }

        [Fact]
        public async Task SubscriptionsRQLSupportIntersect()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = "From Users as u Where intersect(endsWith(u.Name,'nd'), startsWith(u.Name, 'Th'), regex(u.Name, \"^(\\w+\\s+){3}$\"))"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Thor the fabulous is second"
                    });
                    session.Store(new User()
                    {
                        Name = "The emperror"
                    });
                    session.Store(new User()
                    {
                        Name = "Thee the second"
                    });
                    session.Store(new User { Name = "you the second" });
                    session.SaveChanges();
                }

                var subscription = store.Subscriptions.Open<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    amre.Set();
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(2, users.Count);
            }
        }
        

    
    }
}
