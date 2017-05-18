using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Notifications
{
    public class ChangesTests : RavenTestBase
    {
        [Fact]
        public async Task CanGetNotificationAboutDocumentPut()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");
                //await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                DocumentChange documentChange;
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Key);
                Assert.Equal(documentChange.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChange.Etag);
            }
        }

        [Fact]
        public async Task CanGetAllNotificationAboutDocument_ALotOfDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForAllDocuments();
                //await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                const int docsCount = 10000;

                for (int j = 0; j < docsCount / 100; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i <= 100; i++)
                        {
                            await session.StoreAsync(new User(), "users/");
                        }
                        await session.SaveChangesAsync();
                    }
                }

                DocumentChange documentChange;
                int total = docsCount;
                while (total-- > 0)
                    Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(10)));
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutDocumentDelete()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");
                //await observableWithTask.Task;
                observableWithTask
                    .Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    commands.Delete("users/1", null);
                }

                DocumentChange documentChange;
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(2)));

                Assert.Equal("users/1", documentChange.Key);
                Assert.Equal(documentChange.Type, DocumentChangeTypes.Delete);

                //((RemoteDatabaseChanges)taskObservable).DisposeAsync().Wait();
            }
        }

        [Fact]
        public async Task CanCreateMultipleNotificationsOnSingleConnection()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");
                //await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                DocumentChange documentChange;
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(15)));

                observableWithTask = taskObservable.ForDocument("users/2");
                //await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public void NotificationOnWrongDatabase_ShouldNotCrashServer()
        {
            using (var store = GetDocumentStore())
            {
                var mre = new ManualResetEventSlim();

                var taskObservable = store.Changes("does-not-exists");
                taskObservable.OnError += e => mre.Set();

                Assert.True(mre.Wait(TimeSpan.FromSeconds(15)));

                // ensure the db still works
                store.Admin.Send(new GetStatisticsOperation());
            }
        }
    }
}
