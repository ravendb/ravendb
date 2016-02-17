using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Notifications
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
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(3)));

                Assert.Equal("users/1", documentChangeNotification.Key);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChangeNotification.Etag);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutDocumentDelete()
        {
            using (var store = await GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                taskObservable.ConnectionTask.Wait();
                var observableWithTask = taskObservable.ForDocument("users/1");
                observableWithTask.Task.Wait();
                observableWithTask.Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "users/1");
                    session.SaveChanges();
                }

                store.DatabaseCommands.Delete("users/1", null);

                DocumentChangeNotification DocumentChangeNotification;
                Assert.True(list.TryTake(out DocumentChangeNotification, TimeSpan.FromSeconds(2)));

                Assert.Equal("items/1", DocumentChangeNotification.Id);
                Assert.Equal(DocumentChangeNotification.Type, DocumentChangeTypes.Delete);

                ((RemoteDatabaseChanges)taskObservable).DisposeAsync().Wait();
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutDocumentIndexUpdate()
        {
            using (var server = GetNewServer())
            using (var store = NewRemoteDocumentStore(ravenDbServer: server))
            {
                var list = new BlockingCollection<IndexChangeNotification>();
                var taskObservable = store.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForIndex("Raven/DocumentsByEntityName");
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                IndexChangeNotification indexChangeNotification;
                Assert.True(list.TryTake(out indexChangeNotification, TimeSpan.FromSeconds(5)));

                Assert.Equal("Raven/DocumentsByEntityName", indexChangeNotification.Name);
                Assert.Equal(indexChangeNotification.Type, IndexChangeTypes.MapCompleted);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutTransformers()
        {
            using (var server = GetNewServer())
            using (var store = NewRemoteDocumentStore(ravenDbServer: server))
            {
                var list = new BlockingCollection<TransformerChangeNotification>();
                var taskObservable = store.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllTransformers();
                observableWithTask.Task.Wait();
                var subscription = observableWithTask
                    .Subscribe(list.Add);

                store.DatabaseCommands.PutTransformer(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { user.Age, user.Name }"
                });

                TransformerChangeNotification transformerChangeNotification;
                Assert.True(list.TryTake(out transformerChangeNotification, TimeSpan.FromSeconds(5)));

                Assert.Equal("users/selectName", transformerChangeNotification.Name);
                Assert.Equal(transformerChangeNotification.Type, TransformerChangeTypes.TransformerAdded);
                Assert.True(list.Count == 0);

                store.DatabaseCommands.DeleteTransformer(Name);

                Assert.True(list.TryTake(out transformerChangeNotification, TimeSpan.FromSeconds(5)));

                Assert.Equal("users/selectName", transformerChangeNotification.Name);
                Assert.Equal(transformerChangeNotification.Type, TransformerChangeTypes.TransformerRemoved);
                Assert.True(list.Count == 0);

                // now unsubscribe
                subscription.Dispose();
                store.DatabaseCommands.PutTransformer(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { user.Age, user.Name }"
                });

                Assert.False(list.TryTake(out transformerChangeNotification, TimeSpan.FromSeconds(3)));

                store.DatabaseCommands.DeleteTransformer(Name);
                Assert.False(list.TryTake(out transformerChangeNotification, TimeSpan.FromSeconds(3)));


            }
        }
    }
}