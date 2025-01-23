using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

/// <summary>
/// The configuration for the OpenAI API client.
/// </summary>
public sealed class OpenAiSettings
{
    /// <summary>
    /// The API key to used to authenticate with the service.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The service endpoint that the client will send requests to. If not set, the default endpoint will be used.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

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

    public bool HasSettings() => string.IsNullOrWhiteSpace(ApiKey) == false;

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(ApiKey)] = ApiKey,
            [nameof(Endpoint)] = Endpoint,
            [nameof(OrganizationId)] = OrganizationId,
            [nameof(ProjectId)] = ProjectId
        };
}
