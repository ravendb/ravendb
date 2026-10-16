using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client
{
    public class DatabaseChangesTests : ClusterTestBase
    {
        public DatabaseChangesTests(ITestOutputHelper output) : base(output)
        {
        }

        // the changes client waits ~2s before reconnecting after the database is deleted
        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.ChangesApi)]
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

                await AssertWaitForExceptionAsync<DatabaseDoesNotExistException>(async () => await obs.EnsureSubscribedNow());
            }
        }
    }
}
