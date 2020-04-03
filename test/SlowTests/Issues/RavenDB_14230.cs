using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14230 : RavenTestBase
    {
        public RavenDB_14230(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetNotificationAboutTimeSeriesAppend()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<TimeSeriesChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForTimeSeriesOfDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 33);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 22);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutTimeSeriesDelete()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<TimeSeriesChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForTimeSeriesOfDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 33);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Remove(DateTime.MinValue, DateTime.MaxValue);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Delete, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.Null(timeSeriesChange.ChangeVector); // we deleted entire segment
            }
        }

        [Fact]
        public async Task CanSubscribeToTimeSeriesChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                var list = new BlockingCollection<TimeSeriesChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForAllTimeSeries();

                TimeSeriesChange timeSeriesChange;
                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 1);
                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));
                }

                observableWithTask = taskObservable.ForTimeSeries("Likes");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 2);
                        session.TimeSeriesFor("users/1", "Dislikes").Append(DateTime.UtcNow, 3);

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                    Assert.Equal("Likes", timeSeriesChange.Name);
                }

                Assert.False(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                observableWithTask = taskObservable.ForTimeSeriesOfDocument("users/1", "Likes");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 4);
                        session.TimeSeriesFor("users/1", "Dislikes").Append(DateTime.UtcNow, 5);

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                    Assert.Equal("Likes", timeSeriesChange.Name);
                }

                Assert.False(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                observableWithTask = taskObservable.ForTimeSeriesOfDocument("users/1");

                using (observableWithTask.Subscribe(list.Add))
                {
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Likes").Append(DateTime.UtcNow, 6);
                        session.TimeSeriesFor("users/1", "Dislikes").Append(DateTime.UtcNow, 7);

                        await session.SaveChangesAsync();
                    }

                    Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                    Assert.True(timeSeriesChange.Name == "Likes" || timeSeriesChange.Name == "Dislikes");

                    Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                    Assert.True(timeSeriesChange.Name == "Likes" || timeSeriesChange.Name == "Dislikes");
                }
            }
        }
    }
}
