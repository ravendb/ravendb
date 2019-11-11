using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Basic
{
    public class DatabaseNameValidations : RavenTestBase
    {
        public DatabaseNameValidations(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ValidateDatabaseName()
        {
            using (var store = GetDocumentStore())
            {
                var dbName1 = "日本語-שלום-cześć-Привет-مرحبا";
                var dbName2 = "Name.with_allowed-chars.123";

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName1)));
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName2)));

                Assert.Equal(0, store.Maintenance.ForDatabase(dbName1).Send(new GetStatisticsOperation()).CountOfDocuments);
                Assert.Equal(0, store.Maintenance.ForDatabase(dbName2).Send(new GetStatisticsOperation()).CountOfDocuments);
                
                var dbName3 = "._.-._-567";
                var e = Assert.Throws<InvalidOperationException>(() => 
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName3))));
                Assert.Contains("The name '._.-._-567' is not permitted. If a name contains '.' character then it must be surrounded by other allowed characters.", e.Message);

                var dbName4 = "";
                e = Assert.Throws<InvalidOperationException>(() => 
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName4))));
                Assert.Contains("An empty name is forbidden for use!", e.Message);

                var dbName5 = "123*_&*^(ABC)$#";
                e = Assert.Throws<InvalidOperationException>(() => 
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName5))));
                Assert.Contains("The name '123*_&*^(ABC)$#' is not permitted. Only letters, digits and characters ('_', '-', '.') are allowed.", e.Message);
                
                var dbName6 = "lpt1";
                e = Assert.Throws<InvalidOperationException>(() => 
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName6))));
                Assert.Contains("The name 'lpt1' is forbidden for use!", e.Message);
                
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] { dbName1, dbName2 },
                    HardDelete = true,
                    TimeToWaitForConfirmation = TimeSpan.FromSeconds(30)
                }));

                try
                {
                    store.Maintenance.ForDatabase(dbName1).Send(new GetStatisticsOperation());
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }

                try
                {
                    store.Maintenance.ForDatabase(dbName2).Send(new GetStatisticsOperation());
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
