using System.Net;
using System.Text.Json.Nodes;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillSuggestAgentCollection.Name)]
public class AiHelperSuggestAgentEndpointTests(ITestOutputHelper output, QuillAiHelperFixture fixture)
    : QuillAiHelperTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_returns_candidates_and_sends_the_cdc_config()
    {
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data"));
        Assert.Equal("Success", resp.Status);
        Assert.Equal(2, resp.Configurations.Count);

        // opaque request body forwarded to the internal service — stays JsonNode
        var sent = JsonNode.Parse(Mock.LastAgentRequestBody!)!;
        Assert.Equal("CdcBasedAgentConfigSetup", (string?)sent["OperationType"]);
        Assert.Equal("from-data", (string?)sent["Mode"]);
        Assert.NotNull(sent["CdcConfig"]);
        Assert.Null(sent["License"]);
        Assert.Null(sent["CertificateThumbprint"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_prompt_returns_a_single_candidate()
    {
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(AiHelperSamples.BuildAgentConfig()));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest("help shoppers find orders", "from-prompt"));
        Assert.Single(resp.Configurations);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Unknown_slug_returns_404()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestAgentAsync("missing", new SuggestAgentRequest(null, "from-data")));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_unknown_mode()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.SuggestAgentAsync(new SuggestAgentRequest(null, "sideways")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastAgentRequestBody); // the mode guard rejects before the AI hop — no tokens spent
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_without_cdc_config_returns_400()
    {
        await using var app = await NewAppAsync();

        var r = await app.Store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(r.TaskId, OngoingTaskType.CdcSink));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastAgentRequestBody); // the from-data-requires-CDC guard rejects before the AI hop
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_422_when_candidate_is_structurally_invalid()
    {
        // candidate missing the required SystemPrompt
        var invalid = new AiAgentConfiguration { Identifier = "x", Name = "y" };
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(invalid));

        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data")));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Candidate_without_identifier_is_still_a_valid_draft()
    {
        // no Identifier: provisioning server-assigns one
        var noId = new AiAgentConfiguration { Name = "Support", SystemPrompt = "You help." };
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(noId));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data"));
        Assert.Single(resp.Configurations);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_caps_candidates_at_three()
    {
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig(),
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data"));
        Assert.Equal(3, resp.Configurations.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_prompt_caps_candidates_at_one()
    {
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest("help shoppers", "from-prompt"));
        Assert.Single(resp.Configurations);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_surfaces_consent_required_instead_of_granting_consent()
    {
        // mock mirrors the real consent gate
        Mock.RequireConsentForAssist = true;
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(AiHelperSamples.BuildAgentConfig()));

        await using var app = await NewAppAsync();

        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest(null, "from-data"));

        // Accepting the AI service's terms is the operator's call, made in the assistant panel.
        Assert.Equal("ConsentRequired", resp.Status);
        Assert.Empty(resp.Configurations);
        Assert.Equal(0, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Slow_generation_within_timeout_returns_success()
    {
        Mock.AgentResponse = (200, AiHelperSamples.AgentEnvelope(AiHelperSamples.BuildAgentConfig()));
        Mock.AssistDelay = TimeSpan.FromSeconds(2);   // well within the shared host's 30s assist timeout

        await using var app = await NewAppAsync();
        var resp = await app.SuggestAgentAsync(new SuggestAgentRequest("help shoppers find orders", "from-prompt"));
        Assert.Equal("Success", resp.Status);
        Assert.Single(resp.Configurations);
    }
}
