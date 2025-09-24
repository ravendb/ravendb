using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14230 : RavenTestBase
    {
        public RavenDB_14230(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ChangesApi | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetNotificationAboutTimeSeriesAppend(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var date = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(date, 33);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.Equal(date, timeSeriesChange.From);
                Assert.Equal(date, timeSeriesChange.To);
                Assert.NotNull(timeSeriesChange.ChangeVector);

                date = DateTime.UtcNow.EnsureMilliseconds();
                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(date, 22);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);
                Assert.Equal(date, timeSeriesChange.From);
                Assert.Equal(date, timeSeriesChange.To);
            }
        }

        [RavenTheory(RavenTestCategory.ChangesApi | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetNotificationAboutTimeSeriesDelete(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var date = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Append(date, 33);
                    session.TimeSeriesFor("users/1", "Likes").Append(date.AddMinutes(1), 22);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Put, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);
                Assert.Equal(date, timeSeriesChange.From);
                Assert.Equal(date.AddMinutes(1), timeSeriesChange.To);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Delete(date, date);
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Delete, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);
                Assert.Equal(date, timeSeriesChange.From);
                Assert.Equal(date, timeSeriesChange.To);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Likes").Delete();
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out timeSeriesChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", timeSeriesChange.DocumentId);
                Assert.Equal(TimeSeriesChangeTypes.Delete, timeSeriesChange.Type);
                Assert.Equal("Likes", timeSeriesChange.Name);
                Assert.NotNull(timeSeriesChange.ChangeVector);
                Assert.Equal(DateTime.MinValue, timeSeriesChange.From);
                Assert.Equal(DateTime.MaxValue, timeSeriesChange.To);
            }
        }

        [RavenTheory(RavenTestCategory.ChangesApi | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanSubscribeToTimeSeriesChanges(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                Assert.NotNull(timeSeriesChange.CollectionName);
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
