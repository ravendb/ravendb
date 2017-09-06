using System;
using System.Collections.Concurrent;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8448 : RavenTestBase
    {
        [Fact]
        public void ShouldBeAbleToUseIdMethod()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");
                    session.Store(new User(), "users/3");

                    session.SaveChanges();
                }

                store.Subscriptions.Create(new SubscriptionCreationOptions
                {
                    Query = "from Users where id() = 'users/1' OR id() = 'users/3'",
                    Name = "test"
                });

                var users = new BlockingCollection<User>();
                using (var s = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions("test")))
                {
                    GC.KeepAlive(s.Run(batch =>
                    {
                        foreach (var user in batch.Items)
                        {
                            users.Add(user.Result);
                        }
                    }));

                    users.TryTake(out var u, TimeSpan.FromSeconds(10));
                    Assert.Equal("users/1", u.Id);

                    users.TryTake(out u, TimeSpan.FromSeconds(10));
                    Assert.Equal("users/3", u.Id);

                    Assert.Equal(0, users.Count);
                }
            }
        }
    }
}
