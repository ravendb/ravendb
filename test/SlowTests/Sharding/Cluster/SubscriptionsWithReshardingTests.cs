using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class SubscriptionsWithReshardingTests : ClusterTestBase
    {
        public SubscriptionsWithReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions, Skip = "fix after resharding")]
        public async Task ContinueSubscriptionAfterResharding()
        {
            using var store = Sharding.GetDocumentStore();
            var id = await store.Subscriptions.CreateAsync<User>();
            
            var users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var mre = new ManualResetEvent(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                              MaxDocsPerBatch = 5
                         }))
            {
                subscription.AfterAcknowledgment += batch =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        if (users.Add(item.Id) == false)
                        {
                            throw new SubscriberErrorException($"Got same {item.Id} twice");
                        }
                    }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"foo$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User());
                    }

                    session.Store(new User(),"foo$users/8-A");

                    session.SaveChanges();
                }
                
                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"bar$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User());
                    }

                    session.Store(new User(),"bar$users/8-A");

                    session.SaveChanges();
                }

                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await Sharding.Resharding.MoveShardForId(store, "users/8-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"baz$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User());
                    }

                    session.Store(new User(),"baz$users/8-A");


                    session.SaveChanges();
                }

                await Assert.ThrowsAsync<TimeoutException>(() => t.WaitAsync(TimeSpan.FromSeconds(5)));

                var expected = new HashSet<string>();
                for (int i = 1; i < 61; i++)
                {
                    var u = $"users/{i}-A";
                    expected.Add(u);
                }

                expected.Add("foo$users/1-A");
                expected.Add("bar$users/1-A");
                expected.Add("baz$users/1-A");
                expected.Add("foo$users/8-A");
                expected.Add("bar$users/8-A");
                expected.Add("baz$users/8-A");

                foreach (var user in users)
                {
                    expected.Remove(user);
                }
                
                Assert.True(expected.Count == 0, $"Missing {string.Join(Environment.NewLine, expected)}");
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions, Skip = "fix after resharding")]
        public async Task ContinueSubscriptionAfterReshardingInACluster()
        {
            var cluster = await CreateRaftCluster(5, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            });

            var id = await store.Subscriptions.CreateAsync<User>();
            
            var users = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            var mre = new ManualResetEvent(false);
            await using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                         {
                              MaxDocsPerBatch = 5,
                              TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                         }))
            {
                subscription.AfterAcknowledgment += batch =>
                {
                    mre.Set();
                    return Task.CompletedTask;
                };

                var t = subscription.Run(batch =>
                {
                    foreach (var item in batch.Items)
                    {
                        users.TryAdd(item.Id, new HashSet<string>(StringComparer.Ordinal));
                        var cv = users[item.Id];

                        if (cv.Add(item.ChangeVector) == false)
                        {
                            throw new SubscriberErrorException($"Got exact same {item.Id} twice");
                        }
                    }
                });
                

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"foo$users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(),NextId);
                    }

                    session.Store(new User(),"foo$users/8-A");

                    session.SaveChanges();
                }
                
                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 1);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 1);

                await Sharding.Resharding.MoveShardForId(store, "users/1-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"bar$users/1-A");
                    session.Store(new User(),"users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(),"bar$users/8-A");
                    session.Store(new User(),"users/8-A");

                    session.SaveChanges();
                }

                await WaitAndAssertForValueAsync(() => users["users/8-A"].Count, 2);
                await WaitAndAssertForValueAsync(() => users["users/1-A"].Count, 2);

                Assert.True(mre.WaitOne(TimeSpan.FromSeconds(5)));
                mre.Reset();

                await Sharding.Resharding.MoveShardForId(store, "users/8-A");

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"baz$users/1-A");
                    session.Store(new User(),"users/1-A");

                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new User(), NextId);
                    }

                    session.Store(new User(),"baz$users/8-A");
                    session.Store(new User(),"users/8-A");


                    session.SaveChanges();
                }

                await Assert.ThrowsAsync<TimeoutException>(() => t.WaitAsync(TimeSpan.FromSeconds(5)));

                Assert.Equal(3, users["users/1-A"].Count);
                Assert.Equal(3, users["users/8-A"].Count);

                var total = users.Sum(x => x.Value.Count);
                Assert.Equal(70, total);

                var expected = new HashSet<string>();
                for (int i = 1; i < 61; i++)
                {
                    var u = $"users/{i}-A";
                    expected.Add(u);
                }

                expected.Add("foo$users/1-A");
                expected.Add("bar$users/1-A");
                expected.Add("baz$users/1-A");
                expected.Add("foo$users/8-A");
                expected.Add("bar$users/8-A");
                expected.Add("baz$users/8-A");

                foreach (var user in users)
                {
                    expected.Remove(user.Key);
                }
                
                Assert.True(expected.Count == 0, $"Missing {string.Join(Environment.NewLine, expected)}");
            }
        }

        private int _current;
        private string NextId => $"users/{Interlocked.Increment(ref _current)}-A";
    }
}
