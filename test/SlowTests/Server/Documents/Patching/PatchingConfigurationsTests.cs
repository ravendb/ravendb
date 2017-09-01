using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using SlowTests.Core.Utils.Entities;
using Xunit;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchingConfigurationsTests : RavenTestBase
    {
        [Fact]
        public async Task CanReduceCacheSizeButKeepMostUsedScripts()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxNumberOfCachedScripts)] = "20"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Jeroboam" }, "users/1");
                }

                await store.Operations.SendAsync(new PatchOperation<User>("doc", null, new PatchRequest
                {
                    Script = "this.Name = 'Jeroboam0'"
                }));

                for (int i = 1; i < 20; i++)
                {

                    var result = await store.Operations.SendAsync(new PatchOperation<User>("doc", null, new PatchRequest
                    {
                        Script = "this.Name = 'Jeroboam" + i + "';"
                    }));
                }

                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var cacheType = typeof(ScriptRunnerCache);
                var numberOfCahcedScriptsField = cacheType.GetField("_numberOfCachedScripts", BindingFlags.Instance | BindingFlags.NonPublic);
                var numberOfCachedScripts = numberOfCahcedScriptsField.GetValue(db.Scripts);

                Assert.Equal(20, numberOfCachedScripts);

                await store.Operations.SendAsync(new PatchOperation<User>("doc", null, new PatchRequest
                {
                    Script = "this.Name = 'Jeroboam" + 21 + "';"
                }));

                numberOfCachedScripts = numberOfCahcedScriptsField.GetValue(db.Scripts);
                Assert.Equal(16, numberOfCachedScripts);

                await store.Operations.SendAsync(new PatchOperation<User>("doc", null, new PatchRequest
                {
                    Script = "this.Name = 'Jeroboam0'"
                }));

                Assert.Equal(16, numberOfCachedScripts);


            }
        }


        [Fact]
        public async Task CanReuseCachedItem()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxNumberOfCachedScripts)] = "20"
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
                var cacheType = typeof(ScriptRunnerCache);
                var numberOfCahcedScriptsField = cacheType.GetField("_numberOfCachedScripts", BindingFlags.Instance | BindingFlags.NonPublic);
                var numberOfCachedScripts = numberOfCahcedScriptsField.GetValue(db.Scripts);

                Assert.Equal(1, numberOfCachedScripts);
            }
        }

        [Fact]
        public async Task MaximumScriptSteps()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxStepsForScript)] = "8"
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
                    session.Store(new Order() { Lines = Enumerable.Repeat(new OrderLine(), 5).ToList() }, "users/2");
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
