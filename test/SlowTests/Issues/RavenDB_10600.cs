using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10600 : RavenTestBase
    {
        public RavenDB_10600(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TransformScriptShouldWorkWhenAttachmentsArePresentAndShouldBeAbleToSkipDocumentsUsingThrow()
        {
            var exportPath = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");

            DatabaseStatistics initialStats;

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | DatabaseItemType.Attachments | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                Indexes.WaitForIndexing(store);

                initialStats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportPath);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
            }

            using (var store = GetDocumentStore())
            {
                var operation = await store.Smuggler.ImportAsync(
                    new DatabaseSmugglerImportOptions
                    {
                        TransformScript = "this.Value = 123;"
                    },
                    exportPath);

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(initialStats.CountOfAttachments, stats.CountOfAttachments);
                Assert.Equal(initialStats.CountOfDocuments, stats.CountOfDocuments);
                Assert.Equal(initialStats.CountOfUniqueAttachments, stats.CountOfUniqueAttachments);
                //Assert.Equal(initialStats.CountOfRevisionDocuments, stats.CountOfRevisionDocuments);

                using (var commands = store.Commands())
                {
                    var company = await commands.GetAsync("companies/1-A");
                    Assert.True(company.BlittableJson.TryGet("Value", out int value));
                    Assert.Equal(123, value);
                }
            }

            using (var store = GetDocumentStore())
            {
                var operation = await store.Smuggler.ImportAsync(
                    new DatabaseSmugglerImportOptions
                    {
                        TransformScript = "throw 'skip';"
                    },
                    exportPath);

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfAttachments);
                Assert.Equal(0, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfUniqueAttachments);
            }
        }
    }
}
