using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

using static QuillTests.E2E.Fixtures.AgentParameterFixtures;

namespace QuillTests;

[Collection(QuillAgentActionsCollection.Name)]
public class AgentParameterTypeE2ETests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(AiAgentParameterValueType.Default, "42")]
    [InlineData(AiAgentParameterValueType.String, "42")]
    [InlineData(AiAgentParameterValueType.Number, "42")]
    [InlineData(AiAgentParameterValueType.Boolean, "true")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "a,b")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "1,2")]
    [InlineData(AiAgentParameterValueType.ArrayOfBoolean, "true,false")]
    public async Task Embed_chat_works_for_every_declared_parameter_type(
        AiAgentParameterValueType type, string minted)
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var h = await HarnessAsync(mock, type, minted);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
    }

    public static TheoryData<AiAgentParameterValueType, object?> TypedJsonValues() => new()
    {
        { AiAgentParameterValueType.Number, 42 },
        { AiAgentParameterValueType.Boolean, true },
        { AiAgentParameterValueType.ArrayOfString, new[] { "a", "b" } },
        { AiAgentParameterValueType.ArrayOfNumber, new[] { 1, 2 } },
        { AiAgentParameterValueType.ArrayOfBoolean, new[] { true, false } },
    };

    [RavenTheory(RavenTestCategory.Quill)]
    [MemberData(nameof(TypedJsonValues))]
    public async Task Embed_chat_works_when_the_minted_value_is_already_typed_json(
        AiAgentParameterValueType type, object? minted)
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var h = await HarnessAsync(mock, type, minted);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(AiAgentParameterValueType.Number, "not-a-number")]
    [InlineData(AiAgentParameterValueType.Boolean, "maybe")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "1,two")]
    public async Task Mint_rejects_a_value_that_cannot_be_the_declared_type(
        AiAgentParameterValueType type, string minted)
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var (app, channelId) = await ProvisionAsync(mock, type);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channelId, Parameters(("p", minted)), TtlSeconds: 3600, MaxInvocations: 50)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("invalid agent parameter", ex.Body);
        Assert.Contains("p:", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_null_typed_parameter_needs_no_minted_value()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var (app, channelId) = await ProvisionAsync(mock, AiAgentParameterValueType.Null);

        var link = await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channelId, null, TtlSeconds: 3600, MaxInvocations: 50));

        var ndjson = await app.SendEmbedChatAsync(link.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_link_minted_before_the_values_were_json_still_chats()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var (app, channelId) = await ProvisionAsync(mock, AiAgentParameterValueType.Number);

        var link = await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channelId, Parameters(("p", 7)), TtlSeconds: 3600, MaxInvocations: 50));

        using (var session = app.Store.OpenAsyncSession())
        {
            var stored = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + link.Token);
            stored.Parameters["p"] = "7";
            await session.SaveChangesAsync();
        }

        var ndjson = await app.SendEmbedChatAsync(link.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
    }

    private sealed record Harness(QuillApp App, string Token);

    private async Task<Harness> HarnessAsync(
        MockQuillServices mock, AiAgentParameterValueType type, object? mintedValue)
    {
        var (app, channelId) = await ProvisionAsync(mock, type);

        var link = await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(
            channelId,
            Parameters(("p", mintedValue)),
            TtlSeconds: 3600,
            MaxInvocations: 50));

        return new Harness(app, link.Token);
    }

    private async Task<(QuillApp App, string ChannelId)> ProvisionAsync(
        MockQuillServices mock, AiAgentParameterValueType type)
    {
        var app = await NewAppAsync();

        var connectionStringName = "mock-llm-" + Guid.NewGuid().ToString("N");
        await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("test-key", mock.BaseAddress + "/", "mock-model"),
        });

        const string agentId = "typed";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Typed",
            SystemPrompt = "You answer questions.",
            ConnectionStringName =
                ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName),
            Parameters = [new AiAgentParameter { Name = "p", Description = "A parameter.", Type = type }],
        });

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, ["http://localhost"]));

        return (app, channel.ChannelId);
    }
}
