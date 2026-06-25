using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AiApplianceTests.E2E.Fixtures;

/// In-process stand-in for the bundled RavenDB's /quill/ai/assist proxy hop (which in production
/// injects the license + cert and forwards to api.ravendb.net). Hosts the consolidated
/// /quill/ai/assist endpoint and dispatches on the request's OperationType ("CdcConfigSetup" /
/// "AgentConfigSetup"), capturing the last request body per operation so tests can assert the
/// appliance's request shape, and returns a per-test-configurable (status, body) pair. Mirrors
/// <see cref="MockLicenseApi"/>. The caller disposes; the bound base URL is exposed for the
/// appliance via ApplianceOptions.AiApiUrl.
public sealed class MockAiApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    /// Last raw request body received with OperationType "CdcConfigSetup".
    public string? LastCdcRequestBody { get; private set; }

    /// Last raw request body received with OperationType "AgentConfigSetup".
    public string? LastAgentRequestBody { get; private set; }

    /// Response served for the CdcConfigSetup operation (HTTP status code + JSON body).
    public (int Status, string Body) CdcResponse { get; set; } = (200, "{}");

    /// Response served for the AgentConfigSetup operation (HTTP status code + JSON body).
    public (int Status, string Body) AgentResponse { get; set; } = (200, "{}");

    private MockAiApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public static async Task<MockAiApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        // Late-bound holder so route handlers can reach the instance created
        // after the app is built (same shape as MockLicenseApi's closure).
        MockAiApi instance = null!;

        // Consolidated entrypoint: the operation is selected by OperationType in the body. Mapped on
        // both paths so one mock can stand in for either hop: /quill/ai/assist (the appliance's next
        // hop, the bundled RavenDB proxy) and /api/v1/ai/assist (what that proxy forwards to upstream).
        // Inline async block lambdas (not a shared method/local-function) so minimal APIs bind them as
        // IResult-returning route handlers that actually write the response.
        foreach (var path in new[] { "/quill/ai/assist", "/api/v1/ai/assist" })
        {
            app.MapPost(path, async (HttpContext ctx) =>
            {
                // leaveOpen: true so disposing the reader does not dispose the
                // pipeline-owned request body stream.
                using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
                var body = await reader.ReadToEndAsync();

                string? operationType;
                try
                {
                    operationType = (string?)JsonNode.Parse(body)?["OperationType"];
                }
                catch (JsonException)
                {
                    // A non-JSON / empty body throws rather than parsing to null; surface it as the
                    // bad-input 400 the default branch already models instead of a 500.
                    return Results.BadRequest("Malformed JSON body.");
                }

                switch (operationType)
                {
                    case "CdcConfigSetup":
                        instance.LastCdcRequestBody = body;
                        return Results.Content(instance.CdcResponse.Body, "application/json",
                            statusCode: instance.CdcResponse.Status);
                    case "AgentConfigSetup":
                        instance.LastAgentRequestBody = body;
                        return Results.Content(instance.AgentResponse.Body, "application/json",
                            statusCode: instance.AgentResponse.Status);
                    default:
                        return Results.BadRequest($"Unknown OperationType '{operationType}'.");
                }
            });
        }

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockAiApi failed to bind a port.");

        instance = new MockAiApi(app, url.TrimEnd('/'));
        return instance;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
