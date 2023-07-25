using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client;

public class OptimisticConcurrencyAfterResharding : RavenTestBase
{
    public OptimisticConcurrencyAfterResharding(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task OptimisticConcurrencyAfterResharding_DocumentPut()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User
            {
                Name = "aviv"
            }, id);
            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            changeVector = session.Advanced.GetChangeVectorFor(doc);
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            var cvAfterResharding = session.Advanced.GetChangeVectorFor(doc);

            Assert.NotEqual(cvAfterResharding, changeVector);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var versionPart = context.GetChangeVector(cvAfterResharding).Version;
                Assert.Equal(versionPart, changeVector);
            }
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User
            {
                Name = "ayende"
            }, changeVector: changeVector, id);

            // should not throw concurrency exception
            await session.SaveChangesAsync();
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task OptimisticConcurrencyAfterResharding_DocumentDelete()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User(), id);
            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            changeVector = session.Advanced.GetChangeVectorFor(doc);
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var session = store.OpenAsyncSession())
        {
            session.Delete(id, expectedChangeVector: changeVector);

            // should not throw concurrency exception
            await session.SaveChangesAsync();
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task OptimisticConcurrencyAfterResharding_AttachmentPut()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        const string attachmentName = "profile.png";
        const string contentType = "image/png";

        using (var session = store.OpenAsyncSession())
        using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
        {
            await session.StoreAsync(new User(), id);
            session.Advanced.Attachments.Store(id, attachmentName, profileStream, contentType);

            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
            changeVector = attachmentResult.Details.ChangeVector;
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var session = store.OpenAsyncSession())
        {
            var attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
            var cvAfterResharding = attachmentResult.Details.ChangeVector;

            Assert.NotEqual(cvAfterResharding, changeVector);
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var versionPart = context.GetChangeVector(cvAfterResharding).Version;
                Assert.Equal(versionPart, changeVector);
            }
        }

        using (var profileStream = new MemoryStream(new byte[] { 4, 5, 6, 7, 8 }))
        {
            // update attachment stream with optimistic concurrency 
            // should not throw concurrency exception

            var operation = new PutAttachmentOperation(id, attachmentName, profileStream, contentType + "/new", changeVector: changeVector);
            await store.Operations.SendAsync(operation);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task OptimisticConcurrencyAfterResharding_AttachmentPut2()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        const string attachmentName = "profile.png";
        const string contentType = "image/png";

        using (var session = store.OpenAsyncSession())
        using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
        {
            await session.StoreAsync(new User(), id);
            session.Advanced.Attachments.Store(id, attachmentName, profileStream, contentType);

            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
            changeVector = attachmentResult.Details.ChangeVector;
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
        {
            // update attachment name (different casing) with optimistic concurrency 
            // should not throw concurrency exception

            var operation = new PutAttachmentOperation(id, attachmentName.ToUpper(), profileStream, contentType: contentType, changeVector: changeVector);
            await store.Operations.SendAsync(operation);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
    public async Task OptimisticConcurrencyAfterResharding_AttachmentDelete()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        const string attachmentName = "profile.png";
        const string contentType = "image/png";

        using (var session = store.OpenAsyncSession())
        using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
        {
            await session.StoreAsync(new User(), id);
            session.Advanced.Attachments.Store(id, attachmentName, profileStream, contentType);

            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
            changeVector = attachmentResult.Details.ChangeVector;
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        // delete attachment with optimistic concurrency 
        // should not throw concurrency exception

        var operation = new DeleteAttachmentOperation(id, attachmentName, changeVector: changeVector);
        await store.Operations.SendAsync(operation);
    }
}
