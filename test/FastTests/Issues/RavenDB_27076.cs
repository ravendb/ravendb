using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27076 : RavenTestBase
    {
        public RavenDB_27076(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task CallingUndefinedFunctionInPatch_IsNoOp()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Name = "Joe" }, "users/1");
                await s.SaveChangesAsync();
            }

            await store.Operations.SendAsync(new PatchOperation("users/1", null,
                new PatchRequest { Script = "this.Called = someUndefinedFn(this.Name); this.Touched = true;" }));

            using (var s = store.OpenAsyncSession())
            {
                var u = await s.LoadAsync<User>("users/1");
                Assert.NotNull(u);
                Assert.True(u.Touched);
            }
        }

        private sealed class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public object Called { get; set; }
            public bool Touched { get; set; }
        }
    }
}
