using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace InterversionTests.Revisions
{
    // Sharded coverage: legacy raw-CV rows injected per-shard, then orchestrator reads.
    public class ShardedRevisionsMixedTests : RavenTestBase
    {
        public ShardedRevisionsMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ShardedRevisions_AllFourEntities_RoundTripPostMigration()
        {
            using var store = Sharding.GetDocumentStore();

            await RevisionsHelper.SetupRevisionsAsync(store);

            const string docId = "users/sharded-1";

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 2; i++)
            {
                using var session = store.OpenAsyncSession();
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v" + i;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                session.Advanced.Attachments.Store(u, "att-1",
                    new MemoryStream(new byte[] { 1, 2, 3, 4 }), "application/octet-stream");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                session.Advanced.Attachments.Delete(u, "att-1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                Assert.True(metadata.Count >= 4,
                    $"Expected at least 4 revisions for sharded doc {docId}, got {metadata.Count}");
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(docId);
                await session.SaveChangesAsync();
            }
        }
    }
}
