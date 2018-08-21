using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11703 : RavenTestBase
    {
        [Fact]
        public async Task CanGetNotificationAboutCounterIncrement()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<CounterChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForCountersOfDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var counterChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", counterChange.DocumentId);
                Assert.Equal(CounterChangeTypes.Put, counterChange.Type);
                Assert.Equal("Likes", counterChange.Name);
                Assert.Equal(1L, counterChange.Value);
                Assert.NotNull(counterChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", counterChange.DocumentId);
                Assert.Equal(CounterChangeTypes.Increment, counterChange.Type);
                Assert.Equal("Likes", counterChange.Name);
                Assert.Equal(2L, counterChange.Value);
                Assert.NotNull(counterChange.ChangeVector);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutCounterDelete()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<CounterChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForCountersOfDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var counterChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", counterChange.DocumentId);
                Assert.Equal(CounterChangeTypes.Put, counterChange.Type);
                Assert.Equal("Likes", counterChange.Name);
                Assert.Equal(1L, counterChange.Value);
                Assert.NotNull(counterChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", counterChange.DocumentId);
                Assert.Equal(CounterChangeTypes.Delete, counterChange.Type);
                Assert.Equal("Likes", counterChange.Name);
                Assert.Equal(0, counterChange.Value);
                Assert.NotNull(counterChange.ChangeVector);
            }
        }

        [Fact]
        public async Task CanSubscribeToCounterChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                var list = new BlockingCollection<CounterChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForAllCounters();

                CounterChange counterChange;
                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes");
                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));
                }

                observableWithTask = taskObservable.ForCounter("Likes");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes");
                        session.CountersFor("users/1").Increment("Dislikes");

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                    Assert.Equal("Likes", counterChange.Name);
                }

                Assert.False(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                observableWithTask = taskObservable.ForCounterOfDocument("users/1", "Likes");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes");
                        session.CountersFor("users/1").Increment("Dislikes");

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                    Assert.Equal("Likes", counterChange.Name);
                }

                Assert.False(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                observableWithTask = taskObservable.ForCountersOfDocument("users/1");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes");
                        session.CountersFor("users/1").Increment("Dislikes");

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                    Assert.True(counterChange.Name == "Likes" || counterChange.Name == "Dislikes");

                    Assert.True(list.TryTake(out counterChange, TimeSpan.FromSeconds(1)));

                    Assert.True(counterChange.Name == "Likes" || counterChange.Name == "Dislikes");
                }
            }
        }
    }
}
