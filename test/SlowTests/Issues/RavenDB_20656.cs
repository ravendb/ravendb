using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20656 : RavenTestBase
{
    public RavenDB_20656(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_Not_Export_TimeSeries_Counters_Attachments_Revisions_Of_Skipped_Document()
    {
        var exportPath = NewDataPath();
        var exportFile1 = Path.Combine(exportPath, "export1.ravendbdump");
        var exportFile2 = Path.Combine(exportPath, "export2.ravendbdump");

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var company1 = new Company { Id = "companies/1", Name = "HR" };
                var company2 = new Company { Id = "companies/2", Name = "CF" };

                await session.StoreAsync(company1);
                await session.StoreAsync(company2);

                session.TimeSeriesFor(company1, "HR_TS").Append(DateTime.Now, 3);
                session.TimeSeriesFor(company2, "CF_TS").Append(DateTime.Now, 3);

                session.Advanced.Attachments.Store(company1, "A1", new MemoryStream(new byte[] { 1, 2, 3 }));
                session.Advanced.Attachments.Store(company2, "A1", new MemoryStream(new byte[] { 3, 2, 1 }));

                session.CountersFor(company1).Increment("C1", 6);
                session.CountersFor(company2).Increment("C1", 3);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Revisions.ForceRevisionCreationFor("companies/1");
                session.Advanced.Revisions.ForceRevisionCreationFor("companies/2");

                await session.SaveChangesAsync();
            }

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile1);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(2, result.Documents.ReadCount);
            Assert.Equal(0, result.Documents.SkippedCount);
            Assert.Equal(0, result.Documents.ErroredCount);

            Assert.Equal(2, result.Documents.Attachments.ReadCount);
            Assert.Equal(0, result.Documents.Attachments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.SkippedCount);
            Assert.Equal(0, result.RevisionDocuments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.Attachments.ErroredCount);

            Assert.Equal(2, result.Counters.ReadCount);
            Assert.Equal(0, result.Counters.SkippedCount);
            Assert.Equal(0, result.Counters.ErroredCount);

            Assert.Equal(2, result.TimeSeries.ReadCount);
            Assert.Equal(0, result.TimeSeries.SkippedCount);
            Assert.Equal(0, result.TimeSeries.ErroredCount);

            operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
            {
                TransformScript = @"
var name = this.Name;
if (name == 'CF')
   throw 'skip';
"
            }, exportFile2);
            result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(2, result.Documents.ReadCount);
            Assert.Equal(1, result.Documents.SkippedCount);
            Assert.Equal(0, result.Documents.ErroredCount);

            Assert.Equal(1, result.Documents.Attachments.ReadCount);
            Assert.Equal(0, result.Documents.Attachments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.ReadCount);
            Assert.Equal(1, result.RevisionDocuments.SkippedCount);
            Assert.Equal(0, result.RevisionDocuments.ErroredCount);

            Assert.Equal(1, result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.Attachments.ErroredCount);

            Assert.Equal(2, result.Counters.ReadCount);
            Assert.Equal(1, result.Counters.SkippedCount);
            Assert.Equal(0, result.Counters.ErroredCount);

            Assert.Equal(2, result.TimeSeries.ReadCount);
            Assert.Equal(1, result.TimeSeries.SkippedCount);
            Assert.Equal(0, result.TimeSeries.ErroredCount);
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile1);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(2, result.Documents.ReadCount);
            Assert.Equal(0, result.Documents.SkippedCount);
            Assert.Equal(0, result.Documents.ErroredCount);

            Assert.Equal(2, result.Documents.Attachments.ReadCount);
            Assert.Equal(0, result.Documents.Attachments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.SkippedCount);
            Assert.Equal(0, result.RevisionDocuments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.Attachments.ErroredCount);

            Assert.Equal(2, result.Counters.ReadCount);
            Assert.Equal(0, result.Counters.SkippedCount);
            Assert.Equal(0, result.Counters.ErroredCount);

            Assert.Equal(2, result.TimeSeries.ReadCount);
            Assert.Equal(0, result.TimeSeries.SkippedCount);
            Assert.Equal(0, result.TimeSeries.ErroredCount);
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
            {
                TransformScript = @"
var name = this.Name;
if (name == 'CF')
   throw 'skip';
"
            }, exportFile1);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(2, result.Documents.ReadCount);
            Assert.Equal(1, result.Documents.SkippedCount);
            Assert.Equal(0, result.Documents.ErroredCount);

            Assert.Equal(1, result.Documents.Attachments.ReadCount);
            Assert.Equal(0, result.Documents.Attachments.ErroredCount);

            Assert.Equal(2, result.RevisionDocuments.ReadCount);
            Assert.Equal(1, result.RevisionDocuments.SkippedCount);
            Assert.Equal(0, result.RevisionDocuments.ErroredCount);

            Assert.Equal(1, result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.Attachments.ErroredCount);

            Assert.Equal(2, result.Counters.ReadCount);
            Assert.Equal(1, result.Counters.SkippedCount);
            Assert.Equal(0, result.Counters.ErroredCount);

            Assert.Equal(2, result.TimeSeries.ReadCount);
            Assert.Equal(1, result.TimeSeries.SkippedCount);
            Assert.Equal(0, result.TimeSeries.ErroredCount);
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile2);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(1, result.Documents.ReadCount);
            Assert.Equal(0, result.Documents.SkippedCount);
            Assert.Equal(0, result.Documents.ErroredCount);

            Assert.Equal(1, result.Documents.Attachments.ReadCount);
            Assert.Equal(0, result.Documents.Attachments.ErroredCount);

            Assert.Equal(1, result.RevisionDocuments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.SkippedCount);
            Assert.Equal(0, result.RevisionDocuments.ErroredCount);

            Assert.Equal(1, result.RevisionDocuments.Attachments.ReadCount);
            Assert.Equal(0, result.RevisionDocuments.Attachments.ErroredCount);

            Assert.Equal(1, result.Counters.ReadCount);
            Assert.Equal(0, result.Counters.SkippedCount);
            Assert.Equal(0, result.Counters.ErroredCount);

            Assert.Equal(1, result.TimeSeries.ReadCount);
            Assert.Equal(0, result.TimeSeries.SkippedCount);
            Assert.Equal(0, result.TimeSeries.ErroredCount);
        }
    }
}
