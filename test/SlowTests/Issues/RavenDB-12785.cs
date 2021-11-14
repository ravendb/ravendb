using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12785 : RavenTestBase
    {
        public RavenDB_12785(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task CanUseOutputCollectionOnMapReduceJsIndex(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var stream = GetDump("RavenDB-12785.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        TransformScript = @"
var collection = this['@metadata']['@collection'];
if(collection == 'ShoppingCarts')
   throw 'skip';
                    "
                    }, stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var errors = Indexes.WaitForIndexingErrors(store, new[] { "Events/ShoppingCart" }, errorsShouldExists: false);
                    Assert.Null(errors);
                    var count = await session.Advanced.AsyncRawQuery<object>("from ShoppingCarts").CountAsync();
                    Assert.Equal(1, count);
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
