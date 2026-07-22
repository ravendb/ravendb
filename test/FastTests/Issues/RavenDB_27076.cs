using System;
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
        public async Task CallingUndefinedFunctionInPatch_ShouldThrowCleanReferenceError()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Joe" }, "users/1");
                await session.SaveChangesAsync();
            }

            var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
                store.Operations.SendAsync(new PatchOperation("users/1", null,
                    new PatchRequest { Script = "this.Name = someUndefinedFn123(this.Name);" })));

            Assert.Contains("someUndefinedFn123 is not defined", ex.Message);
        }

        private sealed class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
