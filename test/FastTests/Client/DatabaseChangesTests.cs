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
    }
}
