using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Smuggler;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13508 : RavenTestBase
    {
        public RavenDB_13508(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WillNotThrowWhenExportingCollectionThatDoesNotHaveRevisions()
        {
            var exportFile = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration => { });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" });
                    await session.SaveChangesAsync();
                }

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                {
                    Collections = new List<string>
                    {
                        "@hilo"
                    }
                }, exportFile);

                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            }
        }
    }
}
