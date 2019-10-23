using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13709 : RavenTestBase
    {
        public RavenDB_13709(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ImportExistingAutoMapReduceIndexShouldWork()
        {
            var folder = NewDataPath(forceCreateDir: true);
            var file = Path.Combine(folder, "export.ravendbdump");

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());
                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.AsyncRawQuery<dynamic>(@"from Orders
                    group by Company
                        where count() > 5
                    order by count() desc
                        select count() as Count, key() as Company
                    include Company").ToListAsync();

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    var fileInfo = new FileInfo(file);
                    Assert.True(fileInfo.Exists);

                    operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                }
            }
        }
    }
}
