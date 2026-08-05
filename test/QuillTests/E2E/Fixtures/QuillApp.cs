using System.Net;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;

namespace QuillTests.E2E.Fixtures;

public sealed class QuillApp : IAsyncDisposable
{
    private readonly QuillTestBase _parent;
    internal QuillApp(QuillTestBase parent, QuillHost host, IDocumentStore store, string slug)
    {
        Host = host;
        Store = store;
        Slug = slug;
        _parent = parent;

        host.AddApp(this);
    }

    public QuillHost Host { get; }

    public IDocumentStore Store { get; }

    public string Slug { get; }

    public Task WaitForIndexingAsync() => _parent.Indexes.WaitForIndexingAsync(Store);

    public Task<IReadOnlyList<AiConnectionString>> GetConnectionStringsAsync() =>
        Host.GetAppConnectionStringsAsync(Slug);

    public Task<ProvisionAgentResponse> ProvisionAgentAsync(AiAgentConfiguration body) =>
        Host.ProvisionAgentAsync(Slug, body);

    public Task<ProvisionAgentResponse> ProvisionAgentAsync(EditAgentRequest body) =>
        Host.ProvisionAgentAsync(Slug, body);

    public async Task<IReadOnlyList<AgentSummaryResponse>> GetAgentsAsync()
    {
        await WaitForIndexingAsync();
        return await Host.GetAgentsAsync(Slug);
    }

    public Task<AgentDetailsResponse> GetAgentAsync(string agentId) => Host.GetAgentAsync(Slug, agentId);

    public Task<ProvisionAgentResponse> EditAgentAsync(AiAgentConfiguration body) => Host.EditAgentAsync(Slug, body);

    public Task<ProvisionAgentResponse> EditAgentAsync(EditAgentRequest body) => Host.EditAgentAsync(Slug, body);

    public Task DeleteAgentAsync(string agentId) => Host.DeleteAgentAsync(Slug, agentId);

    /// A non-success AI status still returns HTTP 200 with the status on the payload.
    public Task<SuggestAgentResponse> SuggestAgentAsync(SuggestAgentRequest body) => Host.SuggestAgentAsync(Slug, body);

    public Task<ProvisionChannelResponse> ProvisionChannelAsync(ProvisionChannelRequest body) =>
        Host.ProvisionChannelAsync(Slug, body);

    public Task<IReadOnlyList<ChannelSummaryResponse>> GetChannelsAsync() => Host.GetChannelsAsync(Slug);

    public Task<ChannelSummaryResponse> UpdateChannelAsync(string channelId, UpdateChannelRequest body) =>
        Host.UpdateChannelAsync(Slug, channelId, body);

    public Task DeleteChannelAsync(string channelId) => Host.DeleteChannelAsync(Slug, channelId);

    public Task<IReadOnlyList<EmbedLinkSummaryResponse>> GetEmbedLinksAsync() => Host.GetEmbedLinksAsync(Slug);

    public Task<MintEmbedLinkResponse> MintEmbedLinkAsync(MintEmbedLinkRequest body) => Host.MintEmbedLinkAsync(Slug, body);

    public Task RevokeEmbedLinkAsync(string token) => Host.RevokeEmbedLinkAsync(Slug, token);

    public Task<string> GetEmbedPageAsync(string token) => Host.GetEmbedPageAsync(Slug, token);

    /// T=string returns the raw NDJSON body. Optional <paramref name="origin"/> exercises the per-link origin gate.
    public Task<string> SendEmbedChatAsync(string token, string prompt, string? origin = null, CancellationToken ct = default) =>
        Host.SendEmbedChatAsync(Slug, token, prompt, origin, ct);

    public Task<IReadOnlyList<ActivityEventDto>> GetActivityAsync() => Host.GetActivityAsync(Slug);

    public Task<AppOverviewResponse> GetOverviewAsync() => Host.GetOverviewAsync(Slug);

    /// Granularity follows which fields are set: year → months, +month → days, +month+day → hours.
    public async Task<AppUsageResponse> GetUsageAsync(int year, int? month = null, int? day = null)
    {
        await WaitForIndexingAsync();
        return await Host.GetAppUsageAsync(Slug, year, month, day);
    }

    public Task<IReadOnlyList<DataCollectionDto>> GetCollectionsAsync() => Host.GetCollectionsAsync(Slug);

    public Task<ChannelStatsResponse> GetChannelStatsAsync() => Host.GetChannelStatsAsync(Slug);

    public async Task<ConversationStatsResponse> GetConversationStatsAsync(int year, int? month = null, int? day = null)
    {
        await WaitForIndexingAsync();
        return await Host.GetConversationStatsAsync(Slug, year, month, day);
    }

    public async Task<ConversationListResult> GetConversationsAsync(int year, int? start = null, int? pageSize = null)
    {
        await WaitForIndexingAsync();
        return await Host.GetConversationsAsync(Slug, year, start, pageSize);
    }

    /// Caller passes the raw conversation document id, e.g. a percent-encoded <c>chats%2Frecent</c>.
    public Task<ConversationDto> GetConversationAsync(string conversationId) => Host.GetConversationAsync(Slug, conversationId);

    public Task<AppCdcConfigurationResponse> GetCdcAsync() => Host.GetCdcAsync(Slug);

    public Task<CdcPerformanceResponse> GetCdcPerformanceAsync() => Host.GetCdcPerformanceAsync(Slug);

    public Task<IReadOnlyList<CdcError>> GetCdcErrorsAsync() => Host.GetCdcErrorsAsync(Slug);

    public async ValueTask DisposeAsync()
    {
        Host.RemoveApp(this);
        try
        {
            await Host.DeleteAppAsync(Slug);
        }
        catch (QuillHttpException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            // app already gone (e.g. a delete-app test removed it) — teardown is idempotent and must not
        }
    }
}
