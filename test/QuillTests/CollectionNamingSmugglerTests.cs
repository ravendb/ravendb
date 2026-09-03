using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Smuggler;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

// Proves RavenDB's smuggler treats Quill's @-prefixed collections (@channels, @embed-links) like any other:
// export → import round-trips the docs AND the embed-link revision, landing them back in the same @-named collections.
public class CollectionNamingSmugglerTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task At_prefixed_collections_survive_export_import_with_revisions()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo-agent",
            Name = "Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var channelId = (await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }))).ChannelId;
        var token = (await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channelId, [], 3600, 50))).Token;

        var channelDocId = Channel.IdPrefix + channelId;
        var embedLinkDocId = EmbedLink.IdPrefix + token;

        var file = GetTempFileName();
        var export = await app.Store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
        await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        // import into a fresh, convention-less DB: reads below go by id + metadata, so the @-names must survive on their own
        using var target = GetDocumentStore();
        var import = await target.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
        await import.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        using var session = target.OpenAsyncSession();

        var channel = await session.LoadAsync<Channel>(channelDocId);
        Assert.NotNull(channel);
        Assert.Equal("@channels", session.Advanced.GetMetadataFor(channel)["@collection"]!.ToString());

        var link = await session.LoadAsync<EmbedLink>(embedLinkDocId);
        Assert.NotNull(link);
        Assert.Equal("@embed-links", session.Advanced.GetMetadataFor(link)["@collection"]!.ToString());

        var revisions = await session.Advanced.Revisions.GetForAsync<EmbedLink>(embedLinkDocId);
        Assert.NotEmpty(revisions);
    }
}
