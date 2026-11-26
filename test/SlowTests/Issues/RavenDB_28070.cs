using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28070 : RavenTestBase
{
    public RavenDB_28070(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.Revisions)]
    public async Task RevertMustDeleteStreamOfAttachmentThatIsNoLongerReferenced()
    {
        using var store = GetDocumentStore();
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration { Disabled = false }
        });

        var revisionBytes = new byte[] { 1, 2, 3 };
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "v1" }, "users/1");
            session.Advanced.Attachments.Store("users/1", "a", new MemoryStream(revisionBytes));
            await session.SaveChangesAsync();
        }

        string revisionChangeVector;
        using (var session = store.OpenAsyncSession())
        {
            var revisions = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
            revisionChangeVector = revisions[0].GetString(Constants.Documents.Metadata.ChangeVector);
        }

        // no revision will reference the replacement content
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration { Disabled = true }
        });

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Attachments.Store("users/1", "a", new MemoryStream(new byte[] { 4, 5, 6, 7 }));
            await session.SaveChangesAsync();
        }

        Assert.Equal(2, (await store.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfUniqueAttachments);

        await store.Operations.SendAsync(new RevertRevisionsByIdOperation("users/1", revisionChangeVector));

        using (var session = store.OpenAsyncSession())
        using (var attachment = await session.Advanced.Attachments.GetAsync("users/1", "a"))
        {
            var content = new MemoryStream();
            await attachment.Stream.CopyToAsync(content);
            Assert.Equal(revisionBytes, content.ToArray());
        }

        Assert.Equal(1, (await store.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfUniqueAttachments);
    }

    private class User
    {
        public string Name { get; set; }
    }
}
