using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using FastTests.Utils;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11705 : RavenTestBase
    {
        public RavenDB_11705(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanHandleRevisionOperationBeingRolledBack(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: conf =>
                {
                    conf.Default.Disabled = false;
                    conf.Collections.Clear();
                });

                using (var session = store.OpenAsyncSession())
                {
                    // should create the revisions table
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");

                    session.Advanced.Defer(new PatchCommandData("users/2", null,
                        new Raven.Client.Documents.Operations.PatchRequest { Script = "" },
                        // missing doc, this will run
                        new Raven.Client.Documents.Operations.PatchRequest
                        {
                            Script = @"throw 'fail tx';"
                        }));

                    // now the tx is rolled back
                    await Assert.ThrowsAsync<JavaScriptException>(async () => await session.SaveChangesAsync());
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Null(await session.LoadAsync<User>("users/1"));
                    // should re-create the revisions table
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.SaveChangesAsync(); // should work
                }
            }
        }
    }
}
