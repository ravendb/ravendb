using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Server.Config.Settings;
using Voron.Global;
using Xunit;

namespace SlowTests.SchemaUpgrade.Issues
{
    public class RavenDB_12321 : RavenTestBase
    {
        [Theory]
        [InlineData("999")]
        [InlineData("9")]
        public async Task WillThrowSchemaErrorOnBigAndSmallDocumentSchemaAsync(string schemaNum)
        {
            var folder = NewDataPath(forceCreateDir: true);
            int schemaVer;

            using (var store = GetDocumentStore(new Options()
            {
                Path = folder,
                ModifyDatabaseName = s => $"schema_{schemaNum}"
            }))
            {
                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                schemaVer = documentDatabase.DocumentsStorage.Environment.Options.SchemaVersion;
            }

            var path = new PathSetting($"SchemaUpgrade/Issues/DocumentsVersion/schema_{schemaNum}");
            var schemaDir = new DirectoryInfo(path.FullPath);
            Assert.Equal(true, schemaDir.Exists);
            CopyAll(schemaDir, new DirectoryInfo(Path.GetFullPath(folder)));

            var e = Assert.Throws<RavenException>(() =>
                {
                    GetDocumentStore(new Options()
                    {
                        Path = folder
                    });
                });
            Assert.True(e.Message.StartsWith($"Voron.Exceptions.SchemaErrorException: The schema version of this database is expected to be {schemaVer} but is actually {schemaNum}."));
        }

        [Fact]
        public void WillThrowSchemaErrorOnVoronSchema()
        {
            var folder = NewDataPath(forceCreateDir: true);
            int schemaVer;

            using (GetDocumentStore(new Options()
            {
                Path = folder,
                ModifyDatabaseName = s => "schema_999"
            }))
            {
                 schemaVer = Constants.CurrentVersion;
            }

            var path = new PathSetting("SchemaUpgrade/Issues/VoronCurrentVersion/schema_999");
            var schemaDir = new DirectoryInfo(path.FullPath);
            Assert.Equal(true, schemaDir.Exists);
            CopyAll(schemaDir, new DirectoryInfo(Path.GetFullPath(folder)));

            var e = Assert.Throws<RavenException>(() =>
            {
                GetDocumentStore(new Options()
                {
                    Path = folder
                });
            });
            Assert.True(e.Message.StartsWith($"Voron.Exceptions.SchemaErrorException: The db file is for version 999, which is not compatible with the current version {schemaVer}"));
        }

        private static void CopyAll(DirectoryInfo source, DirectoryInfo target)
        {
            foreach (var fi in source.GetFiles())
                fi.CopyTo(Path.Combine(target.FullName, fi.Name), true);

            foreach (var diSourceSubDir in source.GetDirectories())
                CopyAll(diSourceSubDir, target.CreateSubdirectory(diSourceSubDir.Name));
        }
    }
}
