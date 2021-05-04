using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server
{
    public class DatabaseLandlordBugs : RavenLowLevelTestBase
    {
        public DatabaseLandlordBugs(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanCreateAndDeleteDbWithNameBeingPrefix()
        {
            var databaseWithName_2 = CreateDatabaseWithName(true, null, null, "CanCreateAndDeleteDbWithNameBeingPrefix.ctor_2");
            Assert.NotNull(databaseWithName_2);
            var databaseWithName_21 = CreateDatabaseWithName(true, null, null, "CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");
            Assert.NotNull(databaseWithName_21);

            DeleteDatabase("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_2");
            Server.ServerStore.DatabasesLandlord.UnloadDirectly("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");
            var database = await GetDatabase("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");

            Assert.NotNull(database);
        }


        [Fact]
        public void CannotDeleteEntireDbFolder()
        {
            // Checking Core.DataDirectory.FullPath
            var p1 = NewDataPath(forceCreateDir: true);

            var db1 = CreateDatabaseWithName(false, p1, null, "someDB1");
            Assert.NotNull(db1);

            var db2 = CreateDatabaseWithName(false, Path.Combine(p1, "someDB2"), null, "someDB2");
            Assert.NotNull(db2);

            var origFiles = Directory.GetFileSystemEntries(Path.Combine(p1, "someDB2"));

            Assert.Throws<RavenException>(() => DeleteDatabase("someDB1"));

            var currFiles = Directory.GetFileSystemEntries(Path.Combine(p1, "someDB2"));

            Assert.True(CheckFiles(currFiles, origFiles));
        }

        [Fact]
        public void CannotDeleteEntireDbFolder2()
        {
            // Checking Storage.TempPath.FullPath
            var p1 = NewDataPath(forceCreateDir: true);

            var db1 = CreateDatabaseWithName(false, p1, configuration =>
                {
                    configuration.Add(RavenConfiguration.GetKey(x => x.Storage.TempPath), p1);
                }
                , "ravenDB1");
            Assert.NotNull(db1);

            var db2 = CreateDatabaseWithName(false, NewDataPath(forceCreateDir: true), configuration =>
            {
                configuration.Add(RavenConfiguration.GetKey(x => x.Storage.TempPath), Path.Combine(p1, "ravenDB2"));
            }, "ravenDB2");
            Assert.NotNull(db2);

            var origFiles = Directory.GetFileSystemEntries(Path.Combine(p1, "ravenDB2"));

            Assert.Throws<RavenException>(() => DeleteDatabase("ravenDB1"));

            var currFiles = Directory.GetFileSystemEntries(Path.Combine(p1, "ravenDB2"));

            Assert.True(CheckFiles(currFiles, origFiles));
        }

        private static bool CheckFiles(string[] currFiles, string[] origFiles)
        {
            string[] filesWithoutExtension = new string[currFiles.Length];
            int i = 0;
            foreach (var currFile in currFiles)
            {
                filesWithoutExtension[i] = Path.GetFileNameWithoutExtension(currFile);
                i++;
            }
            foreach (var file in origFiles)
            {
                var filename = Path.GetFileNameWithoutExtension(file);
                if (filesWithoutExtension.Contains(filename) == false)
                    return false;
            }
            return true;
        }
    }
}
