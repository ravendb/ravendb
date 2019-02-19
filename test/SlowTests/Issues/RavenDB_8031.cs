using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8031 : RavenTestBase
    {
        [Fact]
        public async Task Can_talk_to_db_if_it_was_created_after_document_store_initialization()
        {
            using (var store = GetDocumentStore())
            {
                var dbName = store.Database + Guid.NewGuid();

                using (var store2 = new DocumentStore()
                {
                    Urls = store.Urls,
                    Database = dbName
                }.Initialize())
                {
                    Assert.Throws<DatabaseDoesNotExistException>(() => store2.Maintenance.Send(new GetStatisticsOperation()));

                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName)));

                    try
                    {
                        for (int i = 0; i < 20; i++)
                        {
                            await Task.Delay(50);

                            try
                            {
                                await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                                return;
                            }
                            catch (Exception)
                            {
                            }
                        }

                        Assert.True(false, "All attempts to get stats failed");
                    }
                    finally
                    {
                        store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, hardDelete: true));
                    }
                }
            }
        }
    }
}
