using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10581 : RavenTestBase
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var commands = store1.Commands())
                {
                    var result = await commands.PutAsync("users/", null, new
                    {
                        Name = "John"
                    });

                    await commands.PutAsync(result.Id, null, new
                    {
                        Name = "Doe"
                    });

                    await commands.PutAsync("users/", null, new
                    {
                        Name = "Bob"
                    });
                }

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store2.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var commands = store2.Commands())
                {
                    await commands.PutAsync("users/", null, new
                    {
                        Name = "Bob"
                    });
                }
            }
        }
    }
}
