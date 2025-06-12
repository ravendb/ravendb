using System;
using System.Threading.Tasks;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace InterversionTests
{
    public class RavenDB_23817 : InterversionTestBase
    {
        public RavenDB_23817(ITestOutputHelper output) : base(output) { }

        [RavenFact(RavenTestCategory.Voron)]
        public async Task UpgradeFrom7To8_ThenRollbackTo7_ShouldFailWhenRollingBack2()
        {
            const string version7 = "7.0.2";
            const string version8 = "8.0.0-nightly-20250603-1852";
            var dbName = GetDatabaseName(nameof(UpgradeFrom7To8_ThenRollbackTo7_ShouldFailWhenRollingBack2));

            var opts = new InterversionTestOptions
            {
                ModifyDatabaseRecord = rec =>
                {
                    rec.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";
                }
            };

            var node = await GetServerAsync(version7, opts, dbName);


            using (var store7 = await GetDocumentStoreAsync(version7, opts, dbName))
            using (var session = store7.OpenAsyncSession())
            {
                await session.StoreAsync(new { Name = "Alice" }, "users/1");
                await session.SaveChangesAsync();
            }

            await UpgradeServerAsync(version8, node);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await UpgradeServerAsync(version7, node);
            });
        }
    }
}

