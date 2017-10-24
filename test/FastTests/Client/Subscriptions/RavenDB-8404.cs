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
        public async Task SubscriptionsRQLSupportStartsWith2()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"declare function funky(doc){

return typeof doc.LastName === 'undefined';
}
From Users as u Where funky(u)"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Thor",
                        LastName = null
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

        [Fact(Skip = "RavenDB_8404")]
        public async Task SubscriptionsRQLSupportEndsWith()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"From Users as u Where endsWith(u.Name,'Th')"
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
                    session.Store(new User { Name = "you" });
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
    }
}
