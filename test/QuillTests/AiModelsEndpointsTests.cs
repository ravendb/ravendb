using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>POST /api/ai/models</c> — the proxy to RavenDB's
/// <c>/studio-tasks/ai/models</c> that backs the connection-string form's model
/// autocomplete. The bundled server forwards the provider's OpenAI-style list
/// response (<c>{ data: [{ id }] }</c>); the appliance flattens it to model ids.
/// </summary>
public class AiModelsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_forwards_request_and_flattens_provider_response()
    {
        var ravendb = new RecordingAiHelperClient(content: """{ "object": "list", "data": [ { "id": "gpt-4o" }, { "id": "gpt-4o-mini" } ] }""");
        using var factory = NewAiModelsFactory(ravendb);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/ai/models", new
        {
            ConnectorType = "OpenAi",
            OpenAiSettings = new { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/" },
        });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        var models = body.GetProperty("models").EnumerateArray().Select(m => m.GetString()!).ToArray();
        Assert.Equal(["gpt-4o", "gpt-4o-mini"], models);

        Assert.Equal("/studio-tasks/ai/models", ravendb.Path);
        Assert.Equal("POST", ravendb.Method);

        // The request is forwarded as-is; AiHelperInternalClient owns the wire serialization.
        var forwarded = Assert.IsType<AiModelsRequest>(ravendb.Request);
        Assert.Equal(AiConnectorType.OpenAi, forwarded.ConnectorType);
        Assert.Equal("sk-test", forwarded.OpenAiSettings?.ApiKey);
        Assert.Equal("https://api.openai.com/v1/", forwarded.OpenAiSettings?.Endpoint);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("Embedded")]   // supported by neither the appliance nor RavenDB's models endpoint
    [InlineData("OpenAi")]     // supported connector but missing its settings object
    public async Task Models_rejects_unsupported_connector_or_missing_settings(string connectorType)
    {
        var ravendb = new RecordingAiHelperClient(content: "{}");
        using var factory = NewAiModelsFactory(ravendb);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/ai/models", new { ConnectorType = connectorType });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(ravendb.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_returns_bad_gateway_when_provider_call_fails()
    {
        var ravendb = new RecordingAiHelperClient(content: string.Empty, transport: AiHelperStatus.InternalError);
        using var factory = NewAiModelsFactory(ravendb);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/ai/models", new
        {
            ConnectorType = "Ollama",
            OllamaSettings = new { Uri = "http://localhost:11434/" },
        });

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Models_returns_bad_gateway_on_unexpected_provider_response()
    {
        var ravendb = new RecordingAiHelperClient(content: "not-json");
        using var factory = NewAiModelsFactory(ravendb);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/ai/models", new
        {
            ConnectorType = "Ollama",
            OllamaSettings = new { Uri = "http://localhost:11434/" },
        });

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
    }

    private WebApplicationFactory<Program> NewAiModelsFactory(IAiHelperClient ravendb)
    {
        var store = GetDocumentStore();
        var baseFactory = NewApplianceFactory(store);
        return baseFactory.WithWebHostBuilder(builder =>
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IAiHelperClient>();
                services.AddSingleton(ravendb);
            }));
    }

    private sealed class RecordingAiHelperClient(string content, AiHelperStatus transport = AiHelperStatus.Success) : IAiHelperClient
    {
        public string? Path { get; private set; }
        public string? Method { get; private set; }
        public object? Request { get; private set; }

        public Task<(AiHelperStatus Transport, string Content)> SendAsync(
            string path,
            string method,
            object request,
            CancellationToken ct)
        {
            Path = path;
            Method = method;
            Request = request;
            return Task.FromResult((transport, content));
        }

        public Task<SuggestCdcInternalResult> SuggestCdcAsync(
            object? schema,
            object? samples,
            string prompt,
            CancellationToken ct) => throw new NotSupportedException();

        public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
            CdcSinkConfiguration cdcConfig,
            object? collectionsSample,
            string mode,
            string? prompt,
            CancellationToken ct) => throw new NotSupportedException();

        public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class =>
            throw new NotSupportedException();
    }
}
