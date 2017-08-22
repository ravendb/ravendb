using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_6971 : RavenTestBase
    {
        [Fact]
        public void Overflow_shrink_needs_to_update_scratch_buffer_page_to_avoid_data_override_after_restart()
        {
            using (var store = GetDocumentStore(path: NewDataPath()))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM Orders" }, new PatchRequest()
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletion(TimeSpan.FromSeconds(30));
                }

                WaitForIndexing(store);

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);

                store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM Orders" }, new PatchRequest()
                {
                    Script = @"put(""orders/"", this);"
                })).WaitForCompletion(TimeSpan.FromSeconds(30));

                WaitForIndexing(store);

                var errors = store.Admin.ForDatabase(store.Database).Send(new GetIndexErrorsOperation());

                Assert.Empty(errors.SelectMany(x => x.Errors));
            }
        }

        [Fact]
        public void Applying_new_diff_requires_to_zero_destination_bytes_first()
        {
            using (var store = GetDocumentStore(path: NewDataPath()))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                store.Admin.Send(new DeleteIndexOperation("Orders/ByCompany"));
                store.Admin.Send(new DeleteIndexOperation("Orders/Totals"));

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM Orders" }, new PatchRequest()
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletion(TimeSpan.FromSeconds(30));
                }

                try
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM Orders" }, new PatchRequest()
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletion(TimeSpan.FromSeconds(10));
                }
                catch (TimeoutException)
                {
                    // expected
                }

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);
                try
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM Orders" }, new PatchRequest()
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletion(TimeSpan.FromSeconds(10));
                }
                catch (TimeoutException)
                {
                    // expected
                }

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(store.Database);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
