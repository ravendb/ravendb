using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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
                    Query = "from Users as u where id(u) = 'users/1' OR id(u) = 'users/3'",
                    Name = "test"
                });

                var users = new BlockingCollection<User>();
                using (var s = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("test") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var t = s.Run(batch =>
                    {
                        foreach (var user in batch.Items)
                        {
                            users.Add(user.Result);
                        }
                    });

                    try
                    {
                        var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromSeconds(10);
                        Assert.True(users.TryTake(out var u, timeout));
                        Assert.Equal("users/1", u.Id);

                        Assert.True(users.TryTake(out u, timeout));
                        Assert.Equal("users/3", u.Id);

                        Assert.Equal(0, users.Count);
                    }
                    catch (Exception)
                    {
                        if (t.IsCompleted)
                            t.Wait();//throw if needed so expose subscription error
                        throw;
                    }
                }
            }
        }
    }
}
