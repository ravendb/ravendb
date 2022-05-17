using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8404:RavenTestBase
    {
        public RavenDB_8404(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);
        
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportStartsWith(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    //in 32bit we seem to send batches of 1 User
                    if (users.Count == 3)
                    {
                        amre.Set();
                    }
                });
                
                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportEndsWith(Options options)
        {            
            using (var store = GetDocumentStore(options))
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    if (users.Count == 3)
                    {
                        amre.Set();
                    }
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportRegex(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
                var amre = new AsyncManualResetEvent();
                var users = new List<User>();
                _ = subscription.Run(x =>
                {
                    users.AddRange(x.Items.Select(i => i.Result));
                    if (users.Count == 3)
                    {
                        amre.Set();
                    }
                });

                Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                Assert.Equal(3, users.Count);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportExists(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportIntersect(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = "From Users as u Where intersect(endsWith(u.Name,'nd'), startsWith(u.Name, 'Th'), regex(u.Name, 'fabulous'))"
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportIntersectWithComplexRegex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = "From Users as u Where intersect(endsWith(u.Name,'nd'), startsWith(u.Name, 'Th'), regex(u.Name, '^(\\\\w+\\\\s+){4}\\\\w+$'))"
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportStartsWithWithEscapedValues(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = "From Users as u Where startsWith(u.Name, 'my\\\\id')"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "my\\id some other foo bar"
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task SubscriptionsRQLSupportEscapedValue(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"

From Users as u Where u.Name = 'my\\id'

"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = @"my\id"
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

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName);
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
    }
}
