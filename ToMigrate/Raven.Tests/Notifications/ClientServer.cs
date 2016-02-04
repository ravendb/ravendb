using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Notifications
{
    public class ClientServer : RavenTest
    {
        const string Name = "users/selectName";

        public class Item
        {
        }

        public override void Dispose()
        {
            base.Dispose();
            GC.Collect(GC.MaxGeneration);
            GC.WaitForPendingFinalizers();
        }

        [Fact]
        public void CanGetNotificationAboutDocumentPut()
        {
            using(GetNewServer())
            {using (var store = new DocumentStore
            {
                Url = "http://localhost:8079",
                Conventions =
                    {
                        FailoverBehavior = FailoverBehavior.FailImmediately
                    }
            }.Initialize())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForDocument("items/1");
                observableWithTask.Task.Wait();
                observableWithTask.Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(3)));

                Assert.Equal("items/1", documentChangeNotification.Id);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChangeNotification.Etag);
            }
                Thread.Sleep(1000);
            }
        }

        [Fact]
        public void CanGetNotificationAboutDocumentDelete()
        {
            using (GetNewServer())
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var taskObservable = store.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForDocument("items/1");
                observableWithTask.Task.Wait();
                observableWithTask
                    .Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                store.DatabaseCommands.Delete("items/1", null);

                DocumentChangeNotification DocumentChangeNotification;
                Assert.True(list.TryTake(out DocumentChangeNotification, TimeSpan.FromSeconds(2)));

                Assert.Equal("items/1", DocumentChangeNotification.Id);
                Assert.Equal(DocumentChangeNotification.Type, DocumentChangeTypes.Delete);

                ((RemoteDatabaseChanges) taskObservable).DisposeAsync().Wait();
            }
        }

        [Fact]
        public void CanGetNotificationAboutDocumentIndexUpdate()
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
        public void CanGetNotificationAboutTransformers()
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
