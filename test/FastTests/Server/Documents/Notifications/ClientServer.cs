using System;

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;
using System.Runtime.InteropServices;
using Xunit;

namespace FastTests.Server.Documents.Notifications
{
    public class NonLinuxFactAttribute
            : FactAttribute
    {
        public NonLinuxFactAttribute()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == true)
            {
                Skip = "Test cannot be run on Linux machine";
            }
        }
    }

    public class ClientServer : RavenTestBase
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

        [Fact]
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
    }
}
