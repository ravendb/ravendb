using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10887 : RavenTestBase
    {
        public RavenDB_10887(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanSetMaxScratchBufferFileSize()
        {
            using (var store = GetDocumentStore(options: new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(k => k.Storage.MaxScratchBufferSize)] = "4"
            }))
            {
                var documentDatabase = await GetDatabase(store.Database);

                Assert.Equal(4 * Constants.Size.Megabyte, documentDatabase.DocumentsStorage.Environment.Options.MaxScratchBufferSize);

                using (var session = store.OpenSession())
                {
                    // create auto index
                    session.Query<User>().Where(x => x.Name == "a").ToList();
                }

                var index = documentDatabase.IndexStore.GetIndexes().First();

                Assert.Equal(4 * Constants.Size.Megabyte, index._indexStorage.Environment().Options.MaxScratchBufferSize);
            }
        }

        [Fact]
        public void SettingMaxScratchBufferSizeMustNotExceed32BitsLimit()
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(NewDataPath()))
            {
                options.ForceUsing32BitsPager = true;
                options.MaxScratchBufferSize = 512 * Constants.Size.Megabyte;

                // 32 MB is the default on 32 bits
                Assert.Equal(32 * Constants.Size.Megabyte, options.MaxScratchBufferSize);
            }
        }

        [Fact]
        public void SettingMaxScratchBufferSizeCanBeLimitedOn32Bits()
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(NewDataPath()))
            {
                options.ForceUsing32BitsPager = true;
                options.MaxScratchBufferSize = 4 * Constants.Size.Megabyte;

                Assert.Equal(4 * Constants.Size.Megabyte, options.MaxScratchBufferSize);
            }
        }
    }
}
