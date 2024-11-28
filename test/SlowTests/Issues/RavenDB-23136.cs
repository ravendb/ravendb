using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23136 : RavenTestBase
    {
        public RavenDB_23136(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.Revisions)]
        public async Task Database_Shouldnt_Create_Redundant_Revisions_On_Document_Import()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                await Database_Shouldnt_Create_Redundant_Revisions_On_Document_Import(file);
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task Database_Shouldnt_Create_Redundant_Revisions_On_Document_Import(string file)
        {
            using (var source = GetDocumentStore())
            {
                // Create a doc with no revisions
                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "NoRevision" }, "Users/1");
                    await session.SaveChangesAsync();
                }

                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
                await RevisionsHelper.SetupRevisions(source, Server.ServerStore, configuration: configuration);

                // Create a doc with 3 revisions
                for (int i = 0; i < 3; i++)
                {
                    using (var session = source.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = $"Revision{i}" }, "Users/2");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = source.OpenAsyncSession())
                {
                    var revisionsCount1 = await session.Advanced.Revisions.GetCountForAsync("Users/1");
                    Assert.Equal(0, revisionsCount1);
                    var revisionsCount2 = await session.Advanced.Revisions.GetCountForAsync("Users/2");
                    Assert.Equal(3, revisionsCount2);
                }

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var dest = GetDocumentStore())
            {
                var importOperation = await dest.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = dest.OpenAsyncSession())
                {
                    var revisionsCount1 = await session.Advanced.Revisions.GetCountForAsync("Users/1");
                    Assert.Equal(0, revisionsCount1);
                    var revisionsCount2 = await session.Advanced.Revisions.GetCountForAsync("Users/2");
                    Assert.Equal(3, revisionsCount2);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
