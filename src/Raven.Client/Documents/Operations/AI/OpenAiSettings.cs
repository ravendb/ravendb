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

    internal override OpenAiApiMode GetApiMode()
    {
        if (ApiMode.HasValue)
            return ApiMode.Value;

        if (string.IsNullOrEmpty(Endpoint))
            return OpenAiApiMode.Responses;

        // OpenAI's own API has /responses. Any other endpoint is a server that may not, and connection strings
        // pointing at one used Chat Completions before ApiMode existed.
        if (Uri.TryCreate(Endpoint, UriKind.Absolute, out var uri) && uri.Host.Equals(OpenAiBaseUri.Host, StringComparison.OrdinalIgnoreCase))
            return OpenAiApiMode.Responses;

        return OpenAiApiMode.ChatCompletions;
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

        if (Seed.HasValue)
            json[nameof(Seed)] = Seed.Value;

        return json;
    }
}

/// <summary>
/// The OpenAI API used for chat requests, see <see cref="OpenAiBaseSettings.ApiMode"/>.
/// </summary>
public enum OpenAiApiMode
{
    /// <summary>The <c>/responses</c> API.</summary>
    Responses,

    /// <summary>The <c>/chat/completions</c> API.</summary>
    ChatCompletions
}

/// <summary>
/// Specifies the reasoning effort level used by supported models.
/// Kept for migration only; use the string-based <see cref="OpenAiBaseSettings.ReasoningEffort"/> instead.
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
