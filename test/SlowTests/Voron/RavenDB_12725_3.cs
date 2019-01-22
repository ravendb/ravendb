using System.IO;
using FastTests;
using FastTests.Voron;
using Raven.Server.Config.Settings;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_12725_3 : StorageTest
    {
        [Fact]
        public void Voron_schema_update_will_update_headers_file_and_bump_version_there()
        {
            var dataDir = RavenTestHelper.NewDataPath(nameof(Voron_schema_update_will_update_headers_file_and_bump_version_there), 0, forceCreateDir: true);

            var path = new PathSetting($"SchemaUpgrade/Issues/DocumentsVersion/schema_9");
            var schemaDir = new DirectoryInfo(path.FullPath);
            Assert.Equal(true, schemaDir.Exists);
            CopyAll(schemaDir, new DirectoryInfo(Path.GetFullPath(dataDir)));

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataDir)))
            {
                unsafe
                {
                    Assert.Equal(Constants.CurrentVersion, env.HeaderAccessor.Get(ptr => ptr->Version));
                }
            }
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
