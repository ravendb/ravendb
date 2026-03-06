using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26020 : RavenTestBase
{
    public RavenDB_26020(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task CanRemoveHasRevisionsFlagUsingDeleteRevisionsOperationById()
    {
        using (var store = GetDocumentStore())
        {
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false
                }
            });

            var user = new User { Name = "Grisha" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            var database = await GetDatabase(store.Database);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, user.Id, DocumentFields.Id);
                Assert.True(doc.Flags.Contain(DocumentFlags.HasRevisions));
            }

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(user.Id));

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, user.Id, DocumentFields.Id);
                Assert.False(doc.Flags.Contain(DocumentFlags.HasRevisions));
            }
        }
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task CanRemoveHasRevisionsFlagUsingDeleteRevisionsOperationByChangeVector()
    {
        using (var store = GetDocumentStore())
        {
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false
                }
            });

            var user = new User { Name = "Grisha" };
            List<string> revisionCvs1;
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();

                var revisionMetadata = await session.Advanced.Revisions.GetMetadataForAsync(user.Id);
                revisionCvs1 = revisionMetadata.Select(x => x[Constants.Documents.Metadata.ChangeVector].ToString()).ToList();
            }

            List<string> revisionCvs2;
            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<User>(user.Id);
                loaded.Name += " Kotler";
                await session.SaveChangesAsync();

                var revisionMetadata = await session.Advanced.Revisions.GetMetadataForAsync(user.Id);
                revisionCvs2 = revisionMetadata.Select(x => x[Constants.Documents.Metadata.ChangeVector].ToString()).ToList();
            }

            var database = await GetDatabase(store.Database);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, user.Id, DocumentFields.Id);
                Assert.True(doc.Flags.Contain(DocumentFlags.HasRevisions));
            }

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(user.Id, revisionCvs1));

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, user.Id, DocumentFields.Id);
                Assert.True(doc.Flags.Contain(DocumentFlags.HasRevisions));
            }

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(user.Id, revisionCvs2));

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = database.DocumentsStorage.Get(context, user.Id, DocumentFields.Id);
                Assert.False(doc.Flags.Contain(DocumentFlags.HasRevisions));
            }
        }
    }
}
