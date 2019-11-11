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
                // Valid database names:
                var validDbName1 = "日本語-שלום-cześć-Привет-مرحبا";
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(validDbName1)));
                Assert.Equal(0, store.Maintenance.ForDatabase(validDbName1).Send(new GetStatisticsOperation()).CountOfDocuments);
                
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] {validDbName1}, HardDelete = true, TimeToWaitForConfirmation = TimeSpan.FromSeconds(30)
                }));
                Assert.Throws<DatabaseDoesNotExistException>(() => store.Maintenance.ForDatabase(validDbName1).Send(new GetStatisticsOperation()));
              
                var validDbName2 = "Name.with_allowed-chars.123";
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(validDbName2)));
                Assert.Equal(0, store.Maintenance.ForDatabase(validDbName2).Send(new GetStatisticsOperation()).CountOfDocuments);
                
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters
                {
                    DatabaseNames = new[] {validDbName2}, HardDelete = true, TimeToWaitForConfirmation = TimeSpan.FromSeconds(30)
                }));
                Assert.Throws<DatabaseDoesNotExistException>(() => store.Maintenance.ForDatabase(validDbName2).Send(new GetStatisticsOperation()));

                // Invalid database names:
                var InvalidDbName3 = "._.-._-567";
                var e = Assert.Throws<InvalidOperationException>(() =>
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(InvalidDbName3))));
                Assert.Contains("The name '._.-._-567' is not permitted. If a name contains '.' character then it must be surrounded by other allowed characters.",
                    e.Message);

                var InvalidDbName4 = "";
                e = Assert.Throws<InvalidOperationException>(() =>
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(InvalidDbName4))));
                Assert.Contains("An empty name is forbidden for use!", e.Message);

                var InvalidDbName5 = "123*_&*^(ABC)$#";
                e = Assert.Throws<InvalidOperationException>(() =>
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(InvalidDbName5))));
                Assert.Contains("The name '123*_&*^(ABC)$#' is not permitted. Only letters, digits and characters ('_', '-', '.') are allowed.", e.Message);

                var InvalidDbName6 = "lpt1";
                e = Assert.Throws<InvalidOperationException>(() =>
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(InvalidDbName6))));
                Assert.Contains("The name 'lpt1' is forbidden for use!", e.Message);
            }
        }
    }
}
