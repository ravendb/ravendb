using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using SlowTests.Core.Utils.Entities;
using Xunit;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchingConfigurationsTests : RavenTestBase
    {
        public PatchingConfigurationsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReuseCachedItem()
        {
            using (var store = GetDocumentStore(new Options
            {
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Jeroboam" }, "users/1");
                }

                for (int i = 0; i < 20; i++)
                {
                    await store.Operations.SendAsync(new PatchOperation<User>("doc", null, new PatchRequest
                    {
                        Script = "this.Name = 'Jeroboam 1'"
                    }));
                }

                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                
                Assert.Equal(1, db.Scripts.NumberOfCachedScripts);
            }
        }

        // for Jint only as V8 doesn't support the restriction on maximum steps
        [Fact]
        public async Task MaximumScriptSteps()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxStepsForScript)] = "30"
            }))
            {
                using (var session = store.OpenSession())
                {
                    var newEntity = new Order() { Lines = Enumerable.Repeat(new OrderLine(), 3).ToList() };
                    session.Store(newEntity, "users/1");
                    session.SaveChanges();
                }

                await store.Operations.SendAsync(new PatchOperation<User>("users/1", null, new PatchRequest
                {
                    Script = @"for (var i=0; i< this.Lines.length; i++){}"
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order() { Lines = Enumerable.Repeat(new OrderLine(), 25).ToList() }, "users/2");
                    session.SaveChanges();
                }

                await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(() => store.Operations.SendAsync(new PatchOperation<User>("users/2", null, new PatchRequest
                {
                    Script = @"for (var i=0; i< this.Lines.length; i++){}"
                })));
            }
        }
    }
}
