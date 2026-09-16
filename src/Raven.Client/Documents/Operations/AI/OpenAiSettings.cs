using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Configuration for the OpenAI API client.
/// </summary>
public sealed class OpenAiSettings : OpenAiBaseSettings
{
    public OpenAiSettings(string apiKey, string endpoint, string model, string organizationId = null, 
        string projectId = null, int? dimensions = null, double? temperature = null,
        string reasoningEffort = null, int? seed = null) : base(apiKey, endpoint, model, dimensions, temperature)
    {
        OrganizationId = organizationId;
        ProjectId = projectId;
        ReasoningEffort = reasoningEffort;
        Seed = seed;
    }

    public OpenAiSettings()
    {
        // deserialization
    }

    private static readonly Uri OpenAiBaseUri = new Uri("https://api.openai.com/");
    public override Uri GetBaseEndpointUri()
    {
        var uri = string.IsNullOrEmpty(Endpoint) ? OpenAiBaseUri : base.GetBaseEndpointUri();
        var uriBuilder = new UriBuilder(uri);

        if (uri.Equals(OpenAiBaseUri))
        {
            uriBuilder.Path += "v1/";
        }

        return uriBuilder.Uri;
    }

    /// <summary>
    /// The value to use for the <c>OpenAI-Organization</c> request header. Users who belong to multiple organizations
    /// can set this value to specify which organization is used for an API request. Usage from these API requests will
    /// count against the specified organization's quota. If not set, the header will be omitted, and the default
    /// organization will be billed. You can change your default organization in your user settings.
    /// <see href="https://platform.openai.com/docs/guides/production-best-practices/setting-up-your-organization">Learn more</see>.
    /// </summary>
    public string OrganizationId { get; set; }

    /// <summary>
    /// The value to use for the <c>OpenAI-Project</c> request header. Users who are accessing their projects through
    /// their legacy user API key can set this value to specify which project is used for an API request. Usage from
    /// these API requests will count as usage for the specified project. If not set, the header will be omitted, and
    /// the default project will be accessed.
    /// </summary>
    public string ProjectId { get; set; }

    /// <summary>
    /// The <c>reasoning_effort</c> to send to the provider, controlling the reasoning depth
    /// of supported models (such as the GPT-5 family). Lower values reduce internal reasoning,
    /// which may improve latency and reduce variability in responses.
    ///
    /// Typical values are <c>none</c>, <c>minimal</c>, <c>low</c>, <c>medium</c>, <c>high</c>,
    /// <c>xhigh</c> and <c>max</c>. Model families accept different subsets, for example
    /// <c>minimal</c> is rejected by <c>gpt-5.1</c> and later, which take <c>none</c> instead.
    /// When not set the field is omitted and the model applies its own default.
    ///
    /// Sent as supplied apart from trimming, and not validated by RavenDB, so new provider values
    /// can be used without waiting for a RavenDB release. Providers match it exactly: OpenAI accepts
    /// lowercase values only. Legacy <see cref="OpenAiReasoningEffort"/> member names persisted by
    /// older clients (e.g. <c>High</c>) are normalized to the provider form by the server.
    /// </summary>
    public string ReasoningEffort { get; set; }

    /// <summary>
    /// Optional seed used to make the model's sampling more reproducible across requests.
    /// When provided, identical inputs and configuration may produce the same outputs
    /// more consistently across runs.
    ///
    /// This improves response stability (for example in automated tests),
    /// but does not guarantee fully deterministic results due to internal model behavior.
    /// </summary>
    public int? Seed { get; set; }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OpenAiSettings openAiSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (OrganizationId != openAiSettings.OrganizationId ||
            ProjectId != openAiSettings.ProjectId)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        if (string.IsNullOrWhiteSpace(OrganizationId) == false)
            json[nameof(OrganizationId)] = OrganizationId;

        if (string.IsNullOrWhiteSpace(ProjectId) == false)
            json[nameof(ProjectId)] = ProjectId;

        if (string.IsNullOrWhiteSpace(ReasoningEffort) == false)
            json[nameof(ReasoningEffort)] = ReasoningEffort;

        if (Seed.HasValue)
            json[nameof(Seed)] = Seed.Value;

        return json;
    }
}

/// <summary>
/// Specifies the reasoning effort level used by supported models.
/// Kept for migration only; use the string-based <see cref="OpenAiSettings.ReasoningEffort"/> instead.
/// </summary>
[Obsolete("Use the string-based OpenAiSettings.ReasoningEffort instead, e.g. OpenAiReasoningEffort.High.ToReasoningEffort().")]
public enum OpenAiReasoningEffort
{
    Minimal,
    Low,
    Medium,
    High
}

public static class OpenAiReasoningEffortExtensions
{
#pragma warning disable CS0618 // the enum stays public for migration
    /// <summary>
    /// Converts a legacy <see cref="OpenAiReasoningEffort"/> value to the lowercase form
    /// expected by the provider, e.g. <see cref="OpenAiReasoningEffort.High"/> to <c>high</c>.
    /// </summary>
    public static string ToReasoningEffort(this OpenAiReasoningEffort reasoningEffort)
    {
        return reasoningEffort.ToString().ToLowerInvariant();
    }
#pragma warning restore CS0618
}
