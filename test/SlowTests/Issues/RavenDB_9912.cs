using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9912 : RavenTestBase
    {
        [Fact]
        public async Task DocsAndAttachmentsDeletionsShouldBeProcessedCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB_9912.1.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(10 + 5 + 1, stats.CountOfDocuments); // 10 documents + 5 legacy attachment documents + 1 hilo
                Assert.Equal(5, stats.CountOfAttachments);

                using (var stream = GetDump("RavenDB_9912.2.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(9 + 4 + 1, stats.CountOfDocuments); // 9 documents + 4 legacy attachment documents + 1 hilo
                Assert.Equal(4, stats.CountOfAttachments);
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
