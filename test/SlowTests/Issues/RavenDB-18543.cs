using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Constants = Raven.Client.Constants;
using Tests.Infrastructure;
using Raven.Server.Documents;
using System.IO;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace SlowTests.Issues
{
    public class RavenDB_18543 : RavenTestBase
    {
        public RavenDB_18543(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DocumentsHasHasRevisionsFlag()
        {
            DoNotReuseServer();

            var path = NewDataPath(forceCreateDir: true);
            var exportDocsFile = Path.Combine(path, "exportDocs.ravendbdump");
            var exportRevisionsFile = Path.Combine(path, "exportRevisions.ravendbdump");

            using (var store = GetDocumentStore())
            {
                CreateSampleDataForTest(store);

                var exportDocs = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.Documents
                }, exportDocsFile);
                await exportDocs.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var exportRevisions = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.RevisionDocuments
                }, exportRevisionsFile);
                await exportRevisions.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                string databaseName = GetDatabaseName();
                Assert.NotEqual(store.Database, databaseName);

                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));

                var importDocs = await store.Smuggler.ForDatabase(databaseName).ImportAsync(new DatabaseSmugglerImportOptions(), exportDocsFile);
                await importDocs.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var importRevisions = await store.Smuggler.ForDatabase(databaseName).ImportAsync(new DatabaseSmugglerImportOptions(), exportRevisionsFile);
                await importRevisions.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = store.OpenSession(databaseName))
                {
                    var orders = session.Query<Order>().ToList();

                    foreach (Order order in orders)
                    {
                        var metadata = session.Advanced.GetMetadataFor(order);
                        var flagsValue = metadata[Constants.Documents.Metadata.Flags];
                        Assert.NotNull(flagsValue);
                        var flags = Enum.Parse<DocumentFlags>(flagsValue.ToString());
                        Assert.True(flags.HasFlag(DocumentFlags.HasRevisions));
                    }
                }
            }
        }

        private void CreateSampleDataForTest(DocumentStore store)
        {
            DatabaseItemType operateOnTypes = DatabaseItemType.Documents
                                              | DatabaseItemType.RevisionDocuments
                                              | DatabaseItemType.Attachments
                                              | DatabaseItemType.CounterGroups
                                              | DatabaseItemType.TimeSeries
                                              | DatabaseItemType.Indexes
                                              | DatabaseItemType.CompareExchange;

            store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes: operateOnTypes));
        }
    }
}
