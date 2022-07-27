using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_6971 : RavenTestBase
    {
        public RavenDB_6971(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Overflow_shrink_needs_to_update_scratch_buffer_page_to_avoid_data_override_after_restart()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = @"FROM Orders UPDATE { put(""orders/"", this); } " })).WaitForCompletion(TimeSpan.FromSeconds(60));
                }

                Indexes.WaitForIndexing(store);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = @"FROM Orders UPDATE { put(""orders/"", this); } " })).WaitForCompletion(TimeSpan.FromSeconds(60));

                Indexes.WaitForIndexing(store);

                Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));
            }
        }

        [Fact]
        public void Applying_new_diff_requires_to_zero_destination_bytes_first()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                store.Maintenance.Send(new DeleteIndexOperation("Orders/ByCompany"));
                store.Maintenance.Send(new DeleteIndexOperation("Orders/Totals"));

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = @"FROM Orders UPDATE { put(""orders/"", this); } " })).WaitForCompletion(TimeSpan.FromSeconds(60));

                }

                try
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = @"FROM Orders UPDATE { put(""orders/"", this); } " })).WaitForCompletion(TimeSpan.FromSeconds(60));
                }
                catch (TimeoutException)
                {
                    // expected
                }

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                try
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = @"FROM Orders UPDATE { put(""orders/"", this); } " })).WaitForCompletion(TimeSpan.FromSeconds(60));

                }
                catch (TimeoutException)
                {
                    // expected
                }

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
