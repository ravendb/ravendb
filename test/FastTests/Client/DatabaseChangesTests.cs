using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class DatabaseChangesTests : ClusterTestBase
    {
        public DatabaseChangesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ChangesApi)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseChanges_WhenRetryAfterCreatingDatabase_ShouldSubscribe(Options options, bool disableTopologyUpdates)
        {
            var server = GetNewServer();

            options.ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = disableTopologyUpdates;
            options.Server = server;
            options.CreateDatabase = false;

            using var store = GetDocumentStore(options);
            await server.ServerStore.EnsureNotPassiveAsync();

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                try
                {
                    await obs.EnsureSubscribedNow();
                }
                catch (DatabaseDoesNotExistException)
                {
                    //ignore
                }
                catch (AggregateException e) when (e.InnerException is DatabaseDoesNotExistException)
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

        [RavenTheory(RavenTestCategory.ChangesApi)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseChanges_WhenTryToReconnectAfterDeletingDatabase_ShouldFailToSubscribe(Options options, bool disableTopologyUpdates)
        {
            options.ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = disableTopologyUpdates;
            using var store = GetDocumentStore(options);

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();
            }

            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);
            using (var changes = store.Changes())
            {
                var message = string.Empty;
                changes.OnError += exception => Volatile.Write(ref message, exception.Message);
                var obs = changes.ForDocumentsInCollection<User>();
                var task = obs.EnsureSubscribedNow();
                var timeout = TimeSpan.FromSeconds(30);
                if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
                    throw new TimeoutException($"{timeout} {message}");

                var e = await Assert.ThrowsAnyAsync<Exception>(() => task);
                if (e is AggregateException ae)
                    e = ae.ExtractSingleInnerException();
                
                Assert.Equal(typeof(DatabaseDoesNotExistException), e.GetType());
            }
        }

        [RavenTheory(RavenTestCategory.ChangesApi)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseChanges_WhenDeleteDatabaseAfterSubscribe_ShouldSetConnectionStateToDatabaseDoesNotExistException(Options options, bool disableTopologyUpdates)
        {
            options.ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = disableTopologyUpdates;
            using var store = GetDocumentStore(options);

            using (var changes = store.Changes())
            {
                var obs = changes.ForDocumentsInCollection<User>();
                await obs.EnsureSubscribedNow();

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true)).ConfigureAwait(false);

                await AssertWaitForExceptionAsync<DatabaseDoesNotExistException>(async () => await obs.EnsureSubscribedNow(), interval: 1000);
            }
        }

        [RavenTheory(RavenTestCategory.ChangesApi)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DatabaseChanges_WhenDisposeDatabaseChanges_ShouldSetConnectionStateDisposed(Options options, bool disableTopologyUpdates)
        {
            options.ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = disableTopologyUpdates;
            using var store = GetDocumentStore(options);

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
