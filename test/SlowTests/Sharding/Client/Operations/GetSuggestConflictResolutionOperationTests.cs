using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Operations;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class GetSuggestConflictResolutionOperationTests : RavenTestBase
    {
        const string _suffix = "suffix";

        public GetSuggestConflictResolutionOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Studio)]
        public async Task CanGetSuggestConflictResolution()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await GetDocumentDatabaseInstanceFor(store, store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);
            }

            for (int i = 0; i < 10; i++)
            {
                var id = $"users/{i}${_suffix}";
                var conflictsResolution = await store.Maintenance.SendAsync(new GetSuggestConflictResolutionOperation(id));
                Assert.NotNull(conflictsResolution);
                Assert.NotNull(conflictsResolution.Document);
                Assert.NotNull(conflictsResolution.Metadata);
            }
        }

        [RavenFact(RavenTestCategory.Studio | RavenTestCategory.Sharding)]
        public async Task CanGetSuggestConflictResolutionForSharding()
        {
            using var store = Sharding.GetDocumentStore();

            int bucket = ShardHelper.GetBucket(_suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await Sharding.GetShardedDocumentDatabaseForBucketAsync(store.Database, bucket);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);
            }

            for (int i = 0; i < 10; i++)
            {
                var id = $"users/{i}${_suffix}";
                var conflictsResolution = await store.Maintenance.SendAsync(new GetSuggestConflictResolutionOperation(id));
                Assert.NotNull(conflictsResolution);
                Assert.NotNull(conflictsResolution.Document);
                Assert.NotNull(conflictsResolution.Metadata);
            }
        }

        private void AddConflicts(DocumentsOperationContext context, DocumentsStorage storage)
        {
            using (context.OpenWriteTransaction())
            {
                // add some conflicts
                for (int i = 0; i < 10; i++)
                {
                    var id = $"users/{i}${_suffix}";
                    var doc = context.ReadObject(new DynamicJsonValue(), id);
                    storage.ConflictsStorage.AddConflict(context, id, DateTime.UtcNow.Ticks, doc, $"incoming-cv-{i}", "users", DocumentFlags.None);
                }

                context.Transaction.Commit();
            }
        }
    }
}
