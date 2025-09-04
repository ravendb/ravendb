using System;
using System.Net.Http;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Server.Documents.AI.Settings;

internal class OpenAiChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private readonly OpenAiSettings _settings;

    public OpenAiChatCompletionClientSettings(OpenAiSettings settings) 
        : base(settings)
    {
        _settings = settings;
    }

    public override Uri GetBaseUri()
    {
        var uri = base.GetBaseUri();
        
        // Remove /v1/ from the URI if it exists
        var uriBuilder = new UriBuilder(uri);
        uriBuilder.Path = uriBuilder.Path.Replace("/v1/", "/");

        return uriBuilder.Uri;
    }

    public override void AddHeaders(HttpRequestMessage request)
    {
        if (string.IsNullOrEmpty(_settings.OrganizationId) == false)
            request.Headers.TryAddWithoutValidation(Constants.Headers.OpenAiOrganization, _settings.OrganizationId);

        if (string.IsNullOrEmpty(_settings.ProjectId) == false)
            request.Headers.TryAddWithoutValidation(Constants.Headers.OpenAiProject, _settings.ProjectId);
    }
}
