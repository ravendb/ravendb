using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Notifications
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

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                DocumentChange documentChange;
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(documentChange.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChange.ChangeVector);
            }
        }

        public async Task CanGetAllNotificationAboutDocument_ALotOfDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForAllDocuments();

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

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

                observableWithTask
                    .Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

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

                Assert.Equal("users/1", documentChange.Id);
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

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                DocumentChange documentChange;
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(15)));

                observableWithTask = taskObservable.ForDocument("users/2");
                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(15)));
            }
        }

        [Fact]
        public async Task NotificationOnWrongDatabase_ShouldNotCrashServer()
        {
            using (var store = GetDocumentStore())
            {                
                var taskObservable = store.Changes("does-not-exists");                                
                Assert.True( await Assert.ThrowsAsync<DatabaseDoesNotExistException>(async () => await taskObservable.EnsureConnectedNow()).WaitAsync(TimeSpan.FromSeconds(15)));                               

                // ensure the db still works
                store.Maintenance.Send(new GetStatisticsOperation());
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutSideBySideIndexReplacement()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<IndexChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForIndex("Users/All");

                observableWithTask.Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.SideBySideReplace)
                        list.Add(x);
                });
                await observableWithTask.EnsureSubscribedNow();

                new UsersIndex().Execute(store);
                WaitForIndexing(store);
                Assert.True(list.Count == 0);

                new UsersIndexChanged().Execute(store);
                WaitForIndexing(store);

                Assert.True(list.TryTake(out var indexChange, TimeSpan.FromSeconds(1)));
                Assert.Equal("Users/All", indexChange.Name);
                Assert.Equal(IndexChangeTypes.SideBySideReplace, indexChange.Type);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutDocumentsStartingWith()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocumentsStartingWith("users/");
                
                observableWithTask.Subscribe(x =>
                {
                    if (x.Type == DocumentChangeTypes.Put)
                        list.Add(x);
                });
                await observableWithTask.EnsureSubscribedNow();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "differentDocumentPrefix/1");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }
                
                Assert.True(list.TryTake(out var documentChange, TimeSpan.FromSeconds(1)));
                Assert.Equal("users/1", documentChange.Id);
                
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));
                Assert.Equal("users/2", documentChange.Id);
            }
        }
        
        [Fact]
        public async Task CanGetNotificationAboutDocumentsFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocumentsInCollection("users");
                
                observableWithTask.Subscribe(x =>
                {
                    if (x.Type == DocumentChangeTypes.Put)
                        list.Add(x);
                });
                await observableWithTask.EnsureSubscribedNow();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee(), "employees/1");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }
                
                Assert.True(list.TryTake(out var documentChange, TimeSpan.FromSeconds(1)));
                Assert.Equal("users/1", documentChange.Id);
                
                Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(1)));
                Assert.Equal("users/2", documentChange.Id);
            }
        }
        
        [Fact]
        public async Task ShouldThrowWhenTryingToGetNotificationAboutDocumentsWithType()
        {
            using (var store = GetDocumentStore())
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();

#pragma warning disable CS0618 // Type or member is obsolete
                Assert.Throws<NotSupportedException>(() => taskObservable.ForDocumentsOfType<Company>());
#pragma warning restore CS0618 // Type or member is obsolete

            }
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "Users/All";

            public UsersIndex()
            {
                Map = users =>
                    from user in users
                    select new { user.Name, user.LastName, user.Age };

            }
        }

        private class UsersIndexChanged : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "Users/All";

            public UsersIndexChanged()
            {
                Map = users =>
                    from user in users
                    select new { user.Name, user.LastName, user.Age, user.AddressId, user.Id };


            }
        }
        
    }
}
