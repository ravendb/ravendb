using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Client.Documents.Operations.AI;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillAiModelsCollection.Name)]
public class AiModelsEndpointsTests(ITestOutputHelper output, QuillAiModelsFixture fixture)
    : QuillAiModelsTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_forwards_request_and_flattens_provider_response()
    {
        AiHelper.Content = """{ "object": "list", "data": [ { "id": "gpt-4o" }, { "id": "gpt-4o-mini" } ] }""";

        var models = await Host.PostAiModelsAsync(new AiModelsRequest
        {
            ConnectorType = AiConnectorType.OpenAi,
            OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/" },
        });

        Assert.Equal(["gpt-4o", "gpt-4o-mini"], models.Models);

        Assert.Equal("/studio-tasks/ai/models", AiHelper.Path);
        Assert.Equal("POST", AiHelper.Method);

        var forwarded = Assert.IsType<AiModelsRequest>(AiHelper.Request);
        Assert.Equal(AiConnectorType.OpenAi, forwarded.ConnectorType);
        Assert.Equal("sk-test", forwarded.OpenAiSettings?.ApiKey);
        Assert.Equal("https://api.openai.com/v1/", forwarded.OpenAiSettings?.Endpoint);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("Embedded")]   // unsupported provider
    [InlineData("OpenAi")]     // supported provider, missing settings
    public async Task Models_rejects_unsupported_connector_or_missing_settings(string connectorType)
    {
        var request = new AiModelsRequest { ConnectorType = Enum.Parse<AiConnectorType>(connectorType) };
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostAiModelsAsync(request));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(AiHelper.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_returns_bad_gateway_when_provider_call_fails()
    {
        AiHelper.Transport = AiHelperStatus.InternalError;

        var request = new AiModelsRequest { ConnectorType = AiConnectorType.Ollama, OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/" } };
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostAiModelsAsync(request));

        Assert.Equal(HttpStatusCode.BadGateway, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_returns_bad_gateway_on_unexpected_provider_response()
    {
        AiHelper.Content = "not-json";

        var request = new AiModelsRequest { ConnectorType = AiConnectorType.Ollama, OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/" } };
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostAiModelsAsync(request));

        Assert.Equal(HttpStatusCode.BadGateway, ex.StatusCode);
    }
}
