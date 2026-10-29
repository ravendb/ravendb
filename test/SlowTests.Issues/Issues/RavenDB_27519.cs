using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27519 : RavenTestBase
    {
        public RavenDB_27519(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task CallChainOnNullCollectionInPatch_StaysUndefined()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Doc { Items = null, Existing = "old" }, "docs/1");
                await s.SaveChangesAsync();
            }

            await store.Operations.SendAsync(new PatchOperation("docs/1", null,
                new PatchRequest
                {
                    Script = @"
this.AllMatch = this.Items.flatMap(x => x).every(x => x === 'a');
this.Existing = this.Items.flatMap(x => x).every(x => x === 'a');
this.TypeOf = typeof this.Items.flatMap(x => x).every(x => x === 'a');
this.IsUndefined = this.Items.flatMap(x => x).every(x => x === 'a') === undefined;
"
                }));

            using (var commands = store.Commands())
            {
                var json = (await commands.GetAsync("docs/1")).BlittableJson;

                Assert.False(json.TryGetMember("AllMatch", out _), "assigning undefined to a new property must not create it");
                Assert.True(json.TryGetMember("Existing", out var existing));
                Assert.Null(existing);
                Assert.True(json.TryGet("TypeOf", out string typeOf));
                Assert.Equal("undefined", typeOf);
                Assert.True(json.TryGet("IsUndefined", out bool isUndefined));
                Assert.True(isUndefined);
            }
        }

        private sealed class Doc
        {
            public List<string> Items { get; set; }
            public string Existing { get; set; }
        }
    }
}
