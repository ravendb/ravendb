using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24415 : RavenTestBase
    {
        public RavenDB_24415(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Can_Get_Missing_attachments()
        {
            var dest = "Can_push_via_filtered_replication_2.0-5";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream($"SlowTests.Data.RavenDB_24415.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            var zipPath = new PathSetting(fullBackupPath);
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, backupPath);

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                RunInMemory = false,
            }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database) { Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" } }));
                var missingAttachments = store.Operations.ForDatabase(store.Database).Send(new GetMissingAttachmentsOperation(Constants.Documents.Collections.AllDocumentsCollection));
                Assert.True(missingAttachments.Documents.Count != 0 || missingAttachments.Revisions.Count != 0);
            }
        }
    }
}
