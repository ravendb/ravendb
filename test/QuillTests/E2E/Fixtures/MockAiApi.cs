using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QuillTests.E2E.Fixtures;

/// In-process stand-in for the bundled RavenDB's /assistant/assist proxy hop (which in production
/// injects the license + cert and forwards to api.ravendb.net). Hosts the consolidated
/// /assistant/assist endpoint and dispatches on the request's OperationType ("CdcConfigSetup" /
/// "CdcBasedAgentConfigSetup"), capturing the last request body per operation so tests can assert the
/// appliance's request shape, and returns a per-test-configurable (status, body) pair. Also hosts
/// /assistant/give-consent and can gate assist behind consent (<see cref="RequireConsentForAssist"/>)
/// to exercise the appliance's sign-consent-then-retry flow. Mirrors <see cref="MockLicenseApi"/>.
/// The caller disposes; the bound base URL is exposed for the appliance via ApplianceOptions.AiApiUrl.
public sealed class MockAiApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    /// Last raw request body received with OperationType "CdcConfigSetup".
    public string? LastCdcRequestBody { get; private set; }

    /// Last raw request body received with OperationType "CdcBasedAgentConfigSetup".
    public string? LastAgentRequestBody { get; private set; }

    /// Response served for the CdcConfigSetup operation (HTTP status code + JSON body).
    public (int Status, string Body) CdcResponse { get; set; } = (200, "{}");

    /// Response served for the CdcBasedAgentConfigSetup operation (HTTP status code + JSON body).
    public (int Status, string Body) AgentResponse { get; set; } = (200, "{}");

    /// When true, assist returns 401 ConsentRequired until give-consent is called — mirrors the real
    /// service gating each assist on a per-(license, cert) consent document.
    public bool RequireConsentForAssist { get; set; }

    /// Response served for /assistant/give-consent. Default 200 Success; set a 401 to simulate a
    /// license rejected by give-consent's own license check.
    public (int Status, string Body) GiveConsentResponse { get; set; } = (200, "{\"Status\":\"Success\"}");

    /// When true, a successful give-consent does NOT open the assist gate — simulates upstream
    /// propagation lag or consent recorded against a different cert thumbprint than assist checks.
    public bool ConsentGrantHasNoEffect { get; set; }

    /// Number of times /assistant/give-consent was called (lets tests assert the retry flow ran).
    public int GiveConsentCallCount { get; private set; }

    private bool _consentGiven;

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

        // The appliance's next hop: the bundled RavenDB's /assistant/assist proxy. The operation is
        // selected by OperationType in the body. Inline async block lambda (not a method/local-function)
        // so minimal APIs bind it as an IResult-returning route handler that actually writes the response.
        app.MapPost("/assistant/assist", async (HttpContext ctx) =>
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
                    if (instance.RequireConsentForAssist && instance._consentGiven == false)
                        return Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401);
                    return Results.Content(instance.CdcResponse.Body, "application/json",
                        statusCode: instance.CdcResponse.Status);
                case "CdcBasedAgentConfigSetup":
                    instance.LastAgentRequestBody = body;
                    if (instance.RequireConsentForAssist && instance._consentGiven == false)
                        return Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401);
                    return Results.Content(instance.AgentResponse.Body, "application/json",
                        statusCode: instance.AgentResponse.Status);
                default:
                    return Results.BadRequest($"Unknown OperationType '{operationType}'.");
            }
        });

        // The appliance posts here (empty body — the real proxy injects license + cert) when assist
        // returns ConsentRequired, then retries the assist. A 200 flips the consent gate open.
        app.MapPost("/assistant/give-consent", () =>
        {
            instance.GiveConsentCallCount++;
            var (status, gbody) = instance.GiveConsentResponse;
            if (status is >= 200 and < 300 && instance.ConsentGrantHasNoEffect == false)
                instance._consentGiven = true;
            return Results.Content(gbody, "application/json", statusCode: status);
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
