using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24553 : RavenTestBase
{
    public RavenDB_24553(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public void CanUpdateDatabaseCompressionAfterServerwideBackup()
    {
        using (var server = GetNewServer())
        using (var store = GetDocumentStore(new Options { Server = server }))
        {
            // Create serverwide backup
            var backupConfig = new ServerWideBackupConfiguration
            {
                Disabled = false,
                FullBackupFrequency = "0 2 * * 0",
                LocalSettings = new LocalSettings
                {
                    FolderPath = NewDataPath()
                }
            };

            store.Maintenance.Server.Send(new PutServerWideBackupConfigurationOperation(backupConfig));

            // Update database compression settings
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            record.DocumentsCompression.CompressAllCollections = true;
            store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));
        }
    }
}
