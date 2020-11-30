using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13697 : RavenTestBase
    {
        public RavenDB_13697(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetValueAfterDbFirstCreation_WithPreviousError()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
            using (var store = GetDocumentStore())
            using (var documentStore = new DocumentStore
            {
                Urls = store.Urls,
                Database = store.Database + "-" + Guid.NewGuid() // Ensure this doesn't exist before testing
            })
            {
                documentStore.Initialize();
                // Subscribing to Changes API before database is created causes the DatabaseDoesNotExistException later on.
                var t = documentStore.Changes()
                    .ForDocumentsInCollection<Version>();

                await Assert.ThrowsAnyAsync<Exception>(() => t.EnsureSubscribedNow().WithCancellation(cts.Token));

                _ = t.Subscribe(x => { });

                // Check if the database exists.
                var getResult = await documentStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(documentStore.Database), cts.Token).ConfigureAwait(false);
                Assert.Null(getResult);

                var dbRecord = new DatabaseRecord(documentStore.Database);
                var operation = new CreateDatabaseOperation(dbRecord);
                try
                {
                    await documentStore.Maintenance.Server.SendAsync(operation, cts.Token).ConfigureAwait(false);

                    Version dbVersion;
                    using (var session = documentStore.OpenAsyncSession())
                    {
                        // should work
                        dbVersion = await session.LoadAsync<Version>("TheVersion", cts.Token).ConfigureAwait(false);
                    }
                }
                finally
                {
                    await documentStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(documentStore.Database, true), cts.Token);
                }
            }
        }
    }
}
