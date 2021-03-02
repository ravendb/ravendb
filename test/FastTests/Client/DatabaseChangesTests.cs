using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class DatabaseChangesTests : RavenTestBase
    {
        public DatabaseChangesTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DatabaseChanges_WhenRetryAfterCreatingDatabase_ShouldSubscribe()
        {
            var database = GetDatabaseName();
            var server = GetNewServer();
            using var store = new DocumentStore {Database = database, Urls = new[] {server.WebUrl}}.Initialize();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                try
                {
                    await obs.EnsureSubscribedNow();
                }
                catch (AggregateException e) when(e.InnerException is DatabaseDoesNotExistException)
                {
                    //ignore
                }
            }

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database))).ConfigureAwait(false);
            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();
            }
        }

        [Fact]
        public async Task DatabaseChanges_WhenTryToReconnectAfterDeletingDatabase_ShouldFailToSubscribe()
        {
            using var store = GetDocumentStore();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();
            }

            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);
            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                var task = obs.EnsureSubscribedNow();
                var timeout = TimeSpan.FromSeconds(10);
                if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
                    throw new TimeoutException($"{timeout}");

                var e = await Assert.ThrowsAsync<AggregateException>(async () => await task);
                Assert.Equal(typeof(DatabaseDoesNotExistException), e.InnerException?.GetType());
            }
        }

        [Fact]
        public async Task DatabaseChanges_WhenDeleteDatabaseAfterSubscribe_ShouldSetConnectionStateToDatabaseDoesNotExistException()
        {
            using var store = GetDocumentStore();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);

                await AssertWaitForExceptionAsync<DatabaseDoesNotExistException>(async () => await obs.EnsureSubscribedNow(), interval: 1000);
            }
        }

        [Fact]
        public async Task DatabaseChanges_WhenDisposeDatabaseChanges_ShouldSetConnectionStateDisposed()
        {
            using var store = GetDocumentStore();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();

                changes.Dispose();
                await Assert.ThrowsAsync<ObjectDisposedException>(async () => await obs.EnsureSubscribedNow());
            }
        }
    }
}
