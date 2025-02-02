using System;
using System.Collections.Generic;
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
        public async Task Import_Should_Create_Revisions_When_Configuration_Is_On(bool withRevisionsConfig)
        {
            DoNotReuseServer();
            var files = new List<string>()
            {
                GetTempFileName(),
                GetTempFileName(),
                GetTempFileName()
            };
            try
            {
                await Import_Should_Create_Revisions_When_Configuration_Is_On_Internal(files, withRevisionsConfig);
            }
            finally
            {
                foreach (var f in files)
                {
                    File.Delete(f);
                }
            }
        }

        private async Task Import_Should_Create_Revisions_When_Configuration_Is_On_Internal(List<string> files, bool withRevisionsConfig)
        {
            using (var source = GetDocumentStore())
            {
                for (int i = 0; i < files.Count; i++)
                {
                    using (var session = source.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = i.ToString() }, "Users/1");
                        await session.SaveChangesAsync();
                    }

                    var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), files[i]);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }
            }

            using (var dest = GetDocumentStore())
            {
                if (withRevisionsConfig)
                {
                    var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
                    await RevisionsHelper.SetupRevisions(dest, Server.ServerStore, configuration: configuration);
                }

                for (int i = 0; i < files.Count; i++)
                {
                    var importOperation = await dest.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), files[i]);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    using (var session = dest.OpenAsyncSession())
                    {
                        var revisionsCount = await session.Advanced.Revisions.GetCountForAsync("Users/1");
                        if (withRevisionsConfig == false)
                            Assert.Equal(0, revisionsCount);
                        else
                            Assert.Equal(i + 1, revisionsCount);
                    }
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
