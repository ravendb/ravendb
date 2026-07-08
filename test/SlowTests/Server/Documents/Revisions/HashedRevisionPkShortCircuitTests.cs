using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace SlowTests.Server.Documents.Revisions
{
    // Behavioral coverage of the HashedRevisionPk born-clean gate (see DESIGN.md §12).
    public class HashedRevisionPkShortCircuitTests : RavenTestBase
    {
        public HashedRevisionPkShortCircuitTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        // Fresh DB advertises HashedRevisionPk -> raw-form rows are NOT surfaced (born-clean gate skips the raw probe).
        [InlineData(/*hasHashedRevisionPkToken:*/ true, /*expectFound:*/ false)]
        // DB with the token stripped -> raw-form fallback stays active and a seeded legacy row is reachable.
        [InlineData(/*hasHashedRevisionPkToken:*/ false, /*expectFound:*/ true)]
        public async Task SeededRawFormRow_VisibilityIsGatedByHashedRevisionPkToken(bool hasHashedRevisionPkToken, bool expectFound)
        {
            Options options = hasHashedRevisionPkToken
                ? new Options()
                : new Options { ModifyDatabaseRecord = StripHashedRevisionPkToken };

            using (DocumentStore store = GetDocumentStore(options))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.Equal(hasHashedRevisionPkToken, database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);

                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string knownChangeVector;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    Raven.Server.Utils.ChangeVector compoundCv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    RevisionLegacyRowSeeder.SeedLegacyRevisionRow(
                        context, database, "users/1", "Users", compoundCv,
                        etag: database.DocumentsStorage.GenerateNextEtag());

                    knownChangeVector = compoundCv.AsString();
                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    Document revision = database.DocumentsStorage.RevisionsStorage.GetRevision(readCtx, knownChangeVector);
                    Assert.Equal(expectFound, revision != null);
                }
            }
        }
    }
}
