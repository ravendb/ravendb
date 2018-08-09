using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11703 : ClusterTestBase
    {
        [Fact]
        public async Task CanGetNotificationAboutCounterIncrement()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Put, documentChange.Type);
                Assert.NotNull(documentChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Counter, documentChange.Type);
                Assert.Equal("Likes", documentChange.CounterName);
                Assert.NotNull(documentChange.ChangeVector);

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Put, documentChange.Type);
                Assert.NotNull(documentChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Counter, documentChange.Type);
                Assert.Equal("Likes", documentChange.CounterName);
                Assert.NotNull(documentChange.ChangeVector);
            }
        }


        [Fact]
        public async Task CanGetNotificationAboutCounterDelete()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Put, documentChange.Type);
                Assert.NotNull(documentChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Counter, documentChange.Type);
                Assert.Equal("Likes", documentChange.CounterName);
                Assert.NotNull(documentChange.ChangeVector);

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Put, documentChange.Type);
                Assert.NotNull(documentChange.ChangeVector);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("likes");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(DocumentChangeTypes.Counter, documentChange.Type);
                Assert.Equal("Likes", documentChange.CounterName);
                Assert.NotNull(documentChange.ChangeVector);

            }
        }
    }


}
