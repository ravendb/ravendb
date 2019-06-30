using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13697 : RavenTestBase
    {
        [Fact]
        public async Task CanGetValueAfterDbFirstCreation_WithPreviousError()
        {
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

                await Assert.ThrowsAnyAsync<Exception>(()=> t.EnsureSubscribedNow());

                t.Subscribe(x => { });

                // Check if the database exists.
                var getResult = await documentStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(documentStore.Database)).ConfigureAwait(false);
                Assert.Null(getResult);

                var dbRecord = new DatabaseRecord(documentStore.Database);
                var operation = new CreateDatabaseOperation(dbRecord);
                try
                {
                    await documentStore.Maintenance.Server.SendAsync(operation).ConfigureAwait(false);

                    Version dbVersion;
                    using (var session = documentStore.OpenAsyncSession())
                    {
                        // should work
                        dbVersion = await session.LoadAsync<Version>("TheVersion").ConfigureAwait(false);
                    }
                }
                finally
                {
                    await documentStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(documentStore.Database, true));
                }
            }
        }
    }
}
