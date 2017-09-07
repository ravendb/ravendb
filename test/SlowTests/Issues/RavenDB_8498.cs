using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8498 : RavenTestBase
    {
        [Fact]
        public void SholdBeAbleToDeleteADatabaseViaRequestBody()
        {
            using (var store = GetDocumentStore())
            {
                var dbName1 = $"{store.Database}_1";
                var dbName2 = $"{store.Database}_2";

                store.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName1)));
                store.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName2)));

                Assert.Equal(0, store.Admin.ForDatabase(dbName1).Send(new GetStatisticsOperation()).CountOfDocuments);
                Assert.Equal(0, store.Admin.ForDatabase(dbName2).Send(new GetStatisticsOperation()).CountOfDocuments);

                store.Admin.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] { dbName1, dbName2 },
                    HardDelete = true,
                    TimeToWaitForConfirmation = TimeSpan.FromSeconds(30)
                }));

                try
                {
                    store.Admin.ForDatabase(dbName1).Send(new GetStatisticsOperation());
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }

                try
                {
                    store.Admin.ForDatabase(dbName2).Send(new GetStatisticsOperation());
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }
            }
        }
    }
}
