using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler;

public class RavenDB_17341 : RavenTestBase
{
    public RavenDB_17341(ITestOutputHelper output) : base(output)
    {
    }

    private void DeployIndex(IDocumentStore store, bool isUpdate = false)
    {
        var first = new IndexDefinition
        {
            Name = "Users_ByName",
            Maps = { "from user in docs.Users select new { user.Name }" },
            Type = IndexType.Map
        };
        
        var second = new IndexDefinition
        {
            Name = "Users_ByName",
            Maps = { "from user in docs.Users select new { user.Name, user.Age }" },
            Type = IndexType.Map
        };

        store.Maintenance.Send(isUpdate == false 
            ? new PutIndexesOperation(first) 
            : new PutIndexesOperation(second));
    }

    [Fact]
    public async Task IndexesAreExportedAndImportedWithIndexHistory()
    {
        const int WaitToCompleteInMin = 1;
        var file = GetTempFileName();
        try
        {
            using (var store1 = GetDocumentStore(new Options {ModifyDatabaseName = s => $"{s}_1"}))
            using (var store2 = GetDocumentStore(new Options {ModifyDatabaseName = s => $"{s}_2"}))
            {
                DeployIndex(store1);
                Indexes.WaitForIndexing(store1);
                DeployIndex(store1, isUpdate: true);
                Indexes.WaitForIndexing(store1);

                var recordToExport = await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store1.Database));
                Assert.Equal(1, recordToExport.IndexesHistory.Count);
                Assert.Equal(2, recordToExport.IndexesHistory["Users_ByName"].Count);

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.DatabaseRecord,
                    OperateOnDatabaseRecordTypes = DatabaseRecordItemType.IndexesHistory | DatabaseRecordItemType.Revisions
                }, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(WaitToCompleteInMin));

                operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                {
                    OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.DatabaseRecord, 
                    OperateOnDatabaseRecordTypes = DatabaseRecordItemType.IndexesHistory
                }, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(WaitToCompleteInMin));

                var recordImported = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                Assert.Equal(1, recordImported.IndexesHistory.Count);
                Assert.Equal(2, recordImported.IndexesHistory["Users_ByName"].Count);
                WaitForUserToContinueTheTest(store2);
            }
        }
        finally
        {
            File.Delete(file);
        }
    }
}
