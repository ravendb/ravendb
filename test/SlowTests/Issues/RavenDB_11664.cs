using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11664 : RavenTestBase
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB_11664.1.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var employee = await session.LoadAsync<Employee>("employees/9-A");
                    Assert.NotNull(employee);

                    session.Delete(employee);

                    await session.SaveChangesAsync();
                }

                using (var stream = GetDump("RavenDB_11664.1.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var employee = await session.LoadAsync<Employee>("employees/9-A");
                    Assert.NotNull(employee);

                    session.Delete(employee);

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanDeleteDocumentWhenCollectionWasChanged()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("Key/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Orders"}
                    });

                    commands.Delete("key/1", null);

                    commands.Put("Key/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Companies"}
                    });

                    var e = Assert.Throws<RavenException>(() => commands.Delete("key/1", null));
                    Assert.Contains("Could not delete document 'key/1' from collection 'Companies' because tombstone", e.Message);

                    var database = await GetDatabase(store.Database);

                    await database.TombstoneCleaner.ExecuteCleanup();

                    commands.Delete("key/1", null);
                }
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
