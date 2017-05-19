using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class OverflowShrinkAndRestart_RavenDB_6971 : RavenTestBase
    {
        [Fact]
        public void Overflow_shrink_needs_to_update_scratch_buffer_page_to_avoid_data_override_after_restart()
        {
            using (var store = GetDocumentStore(path: NewDataPath()))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchCollectionOperation("Orders", new PatchRequest()
                    {
                        Script = @"PutDocument(""orders/"", this);"
                    })).WaitForCompletion();
                }

                WaitForIndexing(store);

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);

                store.Operations.Send(new PatchCollectionOperation("Orders", new PatchRequest()
                {
                    Script = @"PutDocument(""orders/"", this);"
                })).WaitForCompletion();

                WaitForIndexing(store);

                var errors = store.Admin.ForDatabase(store.Database).Send(new GetIndexErrorsOperation());

                Assert.Empty(errors.SelectMany(x => x.Errors));
            }
        }
    }
}