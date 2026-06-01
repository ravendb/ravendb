using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AiApplianceTests.E2E.Fixtures;

/// In-process stand-in for the internal AI service on api.ravendb.net. Hosts the
/// two AI-Helper endpoints, captures the last request body for each so tests can
/// assert license and thumbprint were attached, and returns a per-test-configurable
/// (status, body) pair. Mirrors <see cref="MockLicenseApi"/>. The caller disposes;
/// the bound base URL is exposed for the appliance via ApplianceOptions.AiApiUrl.
public sealed class MockAiApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    /// Last raw request body received on /api/v1/ai/setup/cdc-config.
    public string? LastCdcRequestBody { get; private set; }

    /// Last raw request body received on /api/v1/ai/setup/agent-config.
    public string? LastAgentRequestBody { get; private set; }

    /// Response served for cdc-config (HTTP status code + JSON body).
    public (int Status, string Body) CdcResponse { get; set; } = (200, "{}");

    /// Response served for agent-config (HTTP status code + JSON body).
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

        app.MapPost("/api/v1/ai/setup/cdc-config", async (HttpContext ctx) =>
        {
            // leaveOpen: true so disposing the reader does not dispose the
            // pipeline-owned request body stream.
            using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
            instance.LastCdcRequestBody = await reader.ReadToEndAsync();
            return Results.Content(instance.CdcResponse.Body, "application/json",
                statusCode: instance.CdcResponse.Status);
        });

        app.MapPost("/api/v1/ai/setup/agent-config", async (HttpContext ctx) =>
        {
            using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
            instance.LastAgentRequestBody = await reader.ReadToEndAsync();
            return Results.Content(instance.AgentResponse.Body, "application/json",
                statusCode: instance.AgentResponse.Status);
        });

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
