#nullable enable
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

/// <summary>
/// The configuration for the OpenAI API client.
/// </summary>
public sealed class OpenAiSettings : OpenAiBaseSettings
{
    public OpenAiSettings(string apiKey, string endpoint, string model, string? organizationId = null, string? projectId = null) : base(apiKey, endpoint, model)
    {
        OrganizationId = organizationId;
        ProjectId = projectId;
    }

    /// <summary>
    /// The value to use for the <c>OpenAI-Organization</c> request header. Users who belong to multiple organizations
    /// can set this value to specify which organization is used for an API request. Usage from these API requests will
    /// count against the specified organization's quota. If not set, the header will be omitted, and the default
    /// organization will be billed. You can change your default organization in your user settings.
    /// <see href="https://platform.openai.com/docs/guides/production-best-practices/setting-up-your-organization">Learn more</see>.
    /// </summary>
    public string? OrganizationId { get; set; }

    /// <summary>
    /// The value to use for the <c>OpenAI-Project</c> request header. Users who are accessing their projects through
    /// their legacy user API key can set this value to specify which project is used for an API request. Usage from
    /// these API requests will count as usage for the specified project. If not set, the header will be omitted, and
    /// the default project will be accessed.
    /// </summary>
    public string? ProjectId { get; set; }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        if (string.IsNullOrWhiteSpace(OrganizationId) == false)
            json[OrganizationId] = OrganizationId;

        if (string.IsNullOrWhiteSpace(ProjectId) == false)
            json[ProjectId] = ProjectId;

        return json;
    }
}
