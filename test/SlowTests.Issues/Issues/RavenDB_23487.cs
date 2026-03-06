using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_23487 : RavenTestBase
    {
        public RavenDB_23487(ITestOutputHelper output) : base(output)
        {
        }
        private class User
        {
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public async Task ExportToStream_CanBeImportedIntoAnotherDatabase()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Bob" }, "users/1");
                    session.SaveChanges();
                }

                using (var ms = new MemoryStream())
                {
                    var exportOp = await src.Smuggler.ExportAsync(
                        new DatabaseSmugglerExportOptions(), ms);
                    await exportOp.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                    ms.Position = 0;

                    var importOp = await dest.Smuggler.ImportAsync(
                        new DatabaseSmugglerImportOptions(), ms);
                    await importOp.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                }

                using (var session = dest.OpenSession())
                {
                    var imported = session.Load<User>("users/1");
                    Assert.NotNull(imported);
                    Assert.Equal("Bob", imported.Name);
                }
            }
        }
    }
}

