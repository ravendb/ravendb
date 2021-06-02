using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using MySqlX.XDevAPI;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchByIndexTests : RavenTestBase
    {
        public PatchByIndexTests(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly TimeSpan CancelAfter = TimeSpan.FromMinutes(10);

        [Fact]
        public async Task PatchByIndex_WhenFinish_ShouldFreeInternalUsageMemory()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var index = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { index }));

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User { Name = "John" });
                    }
                    await session.SaveChangesAsync();
                }
                WaitForIndexing(store);

                using (var context = QueryOperationContext.ShortTermSingleUse(database))
                {
                    var query = new IndexQueryServerSide($"FROM index '{index.Name}'");
                    var patch = new PatchRequest("var u = this; u.is = true;", PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

                    var before = context.Documents.AllocatedMemory;
                    await database.QueryRunner.ExecutePatchQuery(
                        query,
                        new QueryOperationOptions { RetrieveDetails = true },
                        patch,
                        query.QueryParameters,
                        context,
                        p => { },
                        new OperationCancelToken(CancelAfter, CancellationToken.None, CancellationToken.None));
                    var after = context.Documents.AllocatedMemory;

                    //In a case of fragmentation, we don't immediately freeing memory so the memory can be a little bit higher
                    const long threshold = 256;
                    Assert.True(Math.Abs(before - after) < threshold);
                }
            }
        }
    }
}
