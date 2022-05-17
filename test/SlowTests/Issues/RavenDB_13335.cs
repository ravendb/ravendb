using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13335 : RavenTestBase
    {
        public RavenDB_13335(ITestOutputHelper output) : base(output)
        {
        }

        public class Test
        {
            public bool Throw { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanDisposeOfClonesEvenIfScriptsFails(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await Store(store, true);
                try
                {
                    await Patch(store);
                }
                catch (JavaScriptException)
                {

                }
                await Store(store);
                await Patch(store);
            }

        }

        private static async Task Store(DocumentStore store, bool @throw = false)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Test
                {
                    Throw = @throw
                }, "foo");
                await session.SaveChangesAsync();
            }
        }
        private static async Task Patch(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new PatchCommandData(
                    id: "foo",
                    changeVector: null,
                    patch:
                    new PatchRequest
                    {
                        Script = @"if(this.Throw)
                                        throw 'bahhh!';"
                    },
                    patchIfMissing: new PatchRequest
                    { Script = @"throw 'Not found!';" }));
                await session.SaveChangesAsync();
            }
        }
    }
}
