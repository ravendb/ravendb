using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Notifications
{
    public class ChangesTests : RavenTestBase
    {
        public ChangesTests(ITestOutputHelper output) : base(output)
        {
        }

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

        [Fact]
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

                const int docsCount = 5000;

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
                Assert.True(await Assert.ThrowsAsync<DatabaseDoesNotExistException>(async () => await taskObservable.EnsureConnectedNow()).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(15)));

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
                Indexes.WaitForIndexing(store);
                Assert.True(list.Count == 0);

                new UsersIndexChanged().Execute(store);
                Indexes.WaitForIndexing(store);

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
                var list = new List<string>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocumentsStartingWith("users/");

                observableWithTask.Subscribe(x =>
                {
                    if (x.Type == DocumentChangeTypes.Put)
                        list.Add(x.Id);
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

                await WaitAndAssertForValueAsync(() => list.Count, 2);
                Assert.Contains("users/1", list);
                Assert.Contains("users/2", list);
            }
        }

        [Fact]
        public async Task CanGetNotificationAboutDocumentsFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                var list = new List<string>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocumentsInCollection("users");

                observableWithTask.Subscribe(x =>
                {
                    if (x.Type == DocumentChangeTypes.Put)
                        list.Add(x.Id);
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

                await WaitAndAssertForValueAsync(() => list.Count, 2);
                Assert.Contains("users/1", list);
                Assert.Contains("users/2", list);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CacheIsUpdatedAfterChangesApiReconnection()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(Server, store.Database);
                var count = 0;
                database.ForTestingPurposesOnly().OnNextMessageChangesApi = (value, webSocket) =>
                {
                    if (value != ChangesClientConnection.AggressiveCachingPulseValue.ValueToSend)
                        return;

                    if (++count == 1)
                    {
                        webSocket.Abort();
                    }
                };

                const string oldName = "Grisha";
                const string newName = "Grisha Kotler";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = oldName
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue))
                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>("users/1");
                    Assert.Equal(oldName, loaded.Name);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var loaded = await session.LoadAsync<User>("users/1");
                    loaded.Name = newName;
                    await session.SaveChangesAsync();
                }

                var value = await WaitForValueAsync(async () =>
                {
                    using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue))
                    using (var session = store.OpenAsyncSession())
                    {
                        var loaded = await session.LoadAsync<User>("users/1");
                        return loaded.Name;
                    }
                }, newName);

                Assert.Equal(newName, value);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CacheIsUpdatedAfterInitialFailure()
        {
            using (var store = GetDocumentStore())
            {
                var newDatabaseName = store.Database + Guid.NewGuid();
                const string oldName = "Grisha";
                const string newName = "Grisha Kotler";

                await Assert.ThrowsAsync<DatabaseDoesNotExistException>(async () =>
                {
                    using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, database: newDatabaseName))
                    {
                    }
                });

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDatabaseName)));

                try
                {
                    using (var session = store.OpenAsyncSession(newDatabaseName))
                    {
                        await session.StoreAsync(new User
                        {
                            Name = oldName
                        }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, database: newDatabaseName))
                    using (var session = store.OpenAsyncSession(newDatabaseName))
                    {
                        var loaded = await session.LoadAsync<User>("users/1");
                        Assert.Equal(oldName, loaded.Name);
                    }

                    using (var session = store.OpenAsyncSession(newDatabaseName))
                    {
                        var loaded = await session.LoadAsync<User>("users/1");
                        loaded.Name = newName;
                        await session.SaveChangesAsync();
                    }

                    var value = await WaitForValueAsync(async () =>
                    {
                        using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, database: newDatabaseName))
                        using (var session = store.OpenAsyncSession(newDatabaseName))
                        {
                            var loaded = await session.LoadAsync<User>("users/1");
                            return loaded.Name;
                        }
                    }, newName);

                    Assert.Equal(newName, value);

                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(newDatabaseName, hardDelete: true));

                    var exception = await WaitForValueAsync(async () =>
                    {
                        try
                        {
                            using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, database: newDatabaseName))
                            {
                            }

                            return false;
                        }
                        catch (DatabaseDoesNotExistException)
                        {
                            return true;
                        }
                    }, true);

                    Assert.True(exception);

                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDatabaseName)));

                    using (var session = store.OpenAsyncSession(newDatabaseName))
                    {
                        await session.StoreAsync(new User
                        {
                            Name = oldName
                        }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    value = await WaitForValueAsync(async () =>
                    {
                        using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, database: newDatabaseName))
                        using (var session = store.OpenAsyncSession(newDatabaseName))
                        {
                            var loaded = await session.LoadAsync<User>("users/1");
                            return loaded.Name;
                        }
                    }, oldName);

                    Assert.Equal(oldName, value);
                }
                finally
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(newDatabaseName, hardDelete: true));
                }
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
