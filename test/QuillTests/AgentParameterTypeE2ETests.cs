using System.Net;
using System.Text.Json;
using FastTests;
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
    [InlineData(AiAgentParameterValueType.Default, "42", "\"42\"")]
    [InlineData(AiAgentParameterValueType.String, "42", "\"42\"")]
    [InlineData(AiAgentParameterValueType.Number, "42", "42")]
    [InlineData(AiAgentParameterValueType.Boolean, "true", "true")]
    [InlineData(AiAgentParameterValueType.ArrayOfString, "a,b", "[\"a\",\"b\"]")]
    [InlineData(AiAgentParameterValueType.ArrayOfNumber, "1,2", "[1,2]")]
    [InlineData(AiAgentParameterValueType.ArrayOfBoolean, "true,false", "[true,false]")]
    public async Task Embed_chat_binds_every_declared_parameter_type_as_that_type(
        AiAgentParameterValueType type, string minted, string expectedJson)
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var h = await HarnessAsync(mock, type, minted);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
        Assert.Equal(expectedJson, await BoundParameterJsonAsync(h.App, h.Token));
    }

    public static TheoryData<AiAgentParameterValueType, object?, string> TypedJsonValues() => new()
    {
        { AiAgentParameterValueType.Number, 42, "42" },
        { AiAgentParameterValueType.Boolean, true, "true" },
        { AiAgentParameterValueType.ArrayOfString, new[] { "a", "b" }, "[\"a\",\"b\"]" },
        { AiAgentParameterValueType.ArrayOfNumber, new[] { 1, 2 }, "[1,2]" },
        { AiAgentParameterValueType.ArrayOfBoolean, new[] { true, false }, "[true,false]" },
    };

    [RavenTheory(RavenTestCategory.Quill)]
    [MemberData(nameof(TypedJsonValues))]
    public async Task Embed_chat_binds_a_minted_value_that_is_already_typed_json(
        AiAgentParameterValueType type, object? minted, string expectedJson)
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var h = await HarnessAsync(mock, type, minted);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.DoesNotContain("\"type\":\"error\"", ndjson);
        Assert.Contains("ok", ndjson);
        Assert.Equal(expectedJson, await BoundParameterJsonAsync(h.App, h.Token));
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
        Assert.Equal("null", await BoundParameterJsonAsync(app, link.Token));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_error_frame_on_the_public_embed_endpoint_hides_the_bound_value()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var (app, channelId) = await ProvisionAsync(mock, AiAgentParameterValueType.Default);

        var link = await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(
            channelId, Parameters(("p", "users/9911")), TtlSeconds: 3600, MaxInvocations: 50));

        var details = await app.GetAgentAsync(AgentId);
        details.Configuration.Parameters =
            [new AiAgentParameter { Name = "p", Description = "A parameter.", Type = AiAgentParameterValueType.Number }];
        await app.EditAgentAsync(details.Configuration);

        var ndjson = await app.SendEmbedChatAsync(link.Token, "hello");

        Assert.Contains("\"type\":\"error\"", ndjson);
        Assert.DoesNotContain("users/9911", ndjson);
        Assert.Contains("not a valid Number", ndjson);
    }

    private static async Task<string> BoundParameterJsonAsync(QuillApp app, string token)
    {
        string conversationId;
        using (var session = app.Store.OpenAsyncSession())
        {
            var stored = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            conversationId = stored.ConversationId
                             ?? throw new InvalidOperationException("the embed link carries no conversation id");
        }

        using var commands = app.Store.Commands();
        var conversation = await commands.GetAsync(conversationId)
                           ?? throw new InvalidOperationException($"no conversation document '{conversationId}'");

        var json = conversation.BlittableJson.ToString();
        using var parsed = JsonDocument.Parse(json);

        if (parsed.RootElement.TryGetProperty("Parameters", out var parameters) == false ||
            parameters.TryGetProperty("p", out var bound) == false ||
            bound.TryGetProperty("Value", out var value) == false)
            throw new InvalidOperationException($"conversation '{conversationId}' binds no parameter 'p': {json}");

        return JsonSerializer.Serialize(value);
    }

    private const string AgentId = "typed";

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

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = AgentId,
            Name = "Typed",
            SystemPrompt = "You answer questions.",
            ConnectionStringName =
                ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName),
            Parameters = [new AiAgentParameter { Name = "p", Description = "A parameter.", Type = type }],
        });

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, AgentId, ["http://localhost"]));

        return (app, channel.ChannelId);
    }
}
