using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Org.BouncyCastle.Asn1.Cms;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Server.Documents.Notifications
{
    public class ChangesTests : RavenTestBase
    {
        [Fact]
        public async Task ChangesAPIWithDatabaseNameThatHasWhitespace()
        {
            var name = "Foo Bar";
            var doc = new DatabaseRecord(name)
            {
                Settings =
                {
                    [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = false.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = NewDataPath(name),
                    [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                    [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                }
            };

            using (var server = GetNewServer())
            using (var store = new DocumentStore
            {
                Urls = UseFiddler(server.WebUrl),
                Database = name,
                Certificate = null
            }.Initialize())
            {
                var result = store.Admin.Server.Send(new CreateDatabaseOperationWithoutNameValidation(doc));
                var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, timeout);

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

        public class CreateDatabaseOperationWithoutNameValidation : IServerOperation<DatabasePutResult>
        {
            private readonly DatabaseRecord _databaseRecord;
            private readonly int _replicationFactor;

            public CreateDatabaseOperationWithoutNameValidation(DatabaseRecord databaseRecord, int replicationFactor = 1)
            {
                _databaseRecord = databaseRecord;
                _replicationFactor = replicationFactor;
            }

            public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new CreateDatabaseCommand(conventions, context, _databaseRecord, this);
            }

            private class CreateDatabaseCommand : RavenCommand<DatabasePutResult>
            {
                private readonly JsonOperationContext _context;
                private readonly CreateDatabaseOperationWithoutNameValidation _createDatabaseOperation;
                private readonly BlittableJsonReaderObject _databaseDocument;
                private readonly string _databaseName;

                public CreateDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, DatabaseRecord databaseRecord,
                    CreateDatabaseOperationWithoutNameValidation createDatabaseOperation)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));

                    _context = context ?? throw new ArgumentNullException(nameof(context));
                    _createDatabaseOperation = createDatabaseOperation;
                    _databaseName = databaseRecord?.DatabaseName ?? throw new ArgumentNullException(nameof(databaseRecord));
                    _databaseDocument = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, conventions, context);
                }
                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/databases?name={_databaseName}";

                    url += "&replication-factor=" + _createDatabaseOperation._replicationFactor;

                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Put,
                        Content = new BlittableJsonContent(stream =>
                        {
                            _context.Write(stream, _databaseDocument);
                        })
                    };

                    return request;
                }

                public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = JsonDeserializationClient.DatabasePutResult(response);
                }

                public override bool IsReadRequest => false;
            }
        }
    }
}
