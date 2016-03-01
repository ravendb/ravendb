using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;
using Xunit;

namespace FastTests.Server.Documents.Notifications
{
    public class ChangesTests : RavenTestBase
    {
        protected override void ModifyStore(DocumentStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }

        [NonLinuxFact]
        public async Task CanGetNotificationAboutDocumentPut()
        {
            using (var store = await GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                await taskObservable.ConnectionTask;
                var observableWithTask = taskObservable.ForDocument("users/1");
                await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChangeNotification.Key);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChangeNotification.Etag);
            }
        }

        [NonLinuxFact]
        public async Task CanGetAllNotificationAboutDocument_ALotOfDocuments()
        {
            using (var store = await GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                await taskObservable.ConnectionTask;
                var observableWithTask = taskObservable.ForAllDocuments();
                await observableWithTask.Task;
                observableWithTask.Subscribe(list.Add);

                const int docsCount = 10000;

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= docsCount; i++)
                    {
                        await session.StoreAsync(new User(), "users/" + i);
                    }
                    await session.SaveChangesAsync();
                }

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(10)));
                Assert.Equal(docsCount - 1, list.Count);
            }
        }

        [NonLinuxFact]
        public async Task CanGetNotificationAboutDocumentDelete()
        {
            using (var store = await GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                await taskObservable.ConnectionTask;
                var observableWithTask = taskObservable.ForDocument("users/1");
                await observableWithTask.Task;
                observableWithTask
                    .Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                store.DatabaseCommands.Delete("users/1", null);

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

                Assert.Equal("users/1", documentChangeNotification.Key);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Delete);

                ((RemoteDatabaseChanges)taskObservable).DisposeAsync().Wait();
            }
        }

        [NonLinuxFact] 
        public async Task NotificationOnWrongDatabase_ShouldNotCrashServer()
        {
            using (var store = await GetDocumentStore())
            {
                var taskObservable = store.Changes("does-not-exists");

                var exception = await Assert.ThrowsAsync<AggregateException>(async () =>
                {
                    await taskObservable.ConnectionTask;
                });

                // ensure the db still works
                store.DatabaseCommands.GetStatistics();
            }
        }
    }
}
