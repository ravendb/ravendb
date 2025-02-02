using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17159 : RavenTestBase
    {
        public RavenDB_17159(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WrongNumberOfRevisionWithTimeSeriesAfterImport()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1"
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = new User {Name = "Name1", LastName = "LastName1"};
                        await session.StoreAsync(user, "users/1");
                        session.TimeSeriesFor("users/1", "heartbeat").Append(DateTime.Today, 100, "aa");
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var session = store2.OpenAsyncSession())
                    {
                        var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                        Assert.Equal(3, revisionsMetadata.Count);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
