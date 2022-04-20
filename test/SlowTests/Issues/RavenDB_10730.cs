using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10730 : RavenLowLevelTestBase
    {
        public RavenDB_10730(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_enable_errored_index()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition = new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    },
                });
                
                var index = (await database.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString())).Instance;

                index.SetState(IndexState.Error); // will also stop the indexing thread

                index.Enable();

                Assert.Equal(IndexState.Normal, index.State);
            }
        }

        [Fact]
        public async Task Can_enable_disabled_index()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition = new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    },
                });
                
                var index = (await database.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString())).Instance;

                index.Disable();
                index.Enable();

                Assert.Equal(IndexState.Normal, index.State);
            }
        }
    }
}
