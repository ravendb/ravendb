using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;
using Xunit;

namespace FastTests.Server.Documents.Notifications
{
    public class ClientServer : RavenTestBase
    {
        protected override void ModifyStore(DocumentStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }

        [Fact]
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
    }
}