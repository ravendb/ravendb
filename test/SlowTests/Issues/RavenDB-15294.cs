using System;
using System.IO.Compression;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15294 : RavenTestBase
    {
        private static void ExtractFile(string path, string resourceName)
        {
            using (var stream = typeof(RavenDB_15223).Assembly.GetManifestResourceStream(resourceName))
            using (var zip = new ZipArchive(stream, ZipArchiveMode.Read))
            {
                zip.ExtractToDirectory(path);
            }
        }

        [Fact]
        public void CanCompactDataFrom4_2()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var resource = "SlowTests.Data.RavenDB_15294.northwind-4.2.103.zip";

            ExtractFile(backupPath, resource);

            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                CreateDatabase = false
            }))
            {
                var databaseName = GetDatabaseName();

                store.BeforeDispose += (sender, args) =>
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, true));
                };

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName)
                {
                    Settings =
                    {
                        ["DataDir"] = backupPath,
                        ["RunInMemory"] = "false"
                    }
                }));

                using (var s = store.OpenSession(databaseName))
                {
                    s.Load<object>("force/db/load");
                }

                WaitForUserToContinueTheTest(store, database: databaseName);
                var op = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    Documents = true,
                    DatabaseName = databaseName
                }));

                op.WaitForCompletion(TimeSpan.FromMinutes(5));
            }
        }

        public RavenDB_15294(ITestOutputHelper output) : base(output)
        {
        }
    }
}
