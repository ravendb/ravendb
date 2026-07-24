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

/// In-process stand-in for the bundled RavenDB's /assistant/assist proxy hop. Dispatches on the request's
/// OperationType, capturing the last request body per operation and returning a per-test-configurable
/// (status, body) pair. Gates assist behind consent (<see cref="RequireConsentForAssist"/>) to exercise the
/// appliance's sign-consent-then-retry flow, mirroring the real service. Caller disposes.
public sealed class MockAiApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    public string? LastCdcRequestBody { get; private set; }

    public string? LastAgentRequestBody { get; private set; }

    public (int Status, string Body) CdcResponse { get; set; } = (200, "{}");

    public (int Status, string Body) AgentResponse { get; set; } = (200, "{}");

    /// Simulates slow LLM generation.
    public TimeSpan AssistDelay { get; set; }

    /// When true, assist returns 401 ConsentRequired until give-consent is called.
    public bool RequireConsentForAssist { get; set; }

    public (int Status, string Body) GiveConsentResponse { get; set; } = (200, "{\"Status\":\"Success\"}");

    /// When true, a successful give-consent does NOT open the assist gate — simulates propagation lag.
    public bool ConsentGrantHasNoEffect { get; set; }

    public int GiveConsentCallCount { get; private set; }

    private bool _consentGiven;

    private MockAiApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public void Reset()
    {
        LastCdcRequestBody = null;
        LastAgentRequestBody = null;
        CdcResponse = (200, "{}");
        AgentResponse = (200, "{}");
        AssistDelay = TimeSpan.Zero;
        RequireConsentForAssist = false;
        GiveConsentResponse = (200, "{\"Status\":\"Success\"}");
        ConsentGrantHasNoEffect = false;
        GiveConsentCallCount = 0;
        _consentGiven = false;
    }

    public static async Task<MockAiApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        // late-bound holder so route handlers can reach the instance built below
        MockAiApi instance = null!;

        // inline async block lambda (not a method/local function) so minimal APIs bind it as an
        // IResult-returning route handler that actually writes the response
        app.MapPost("/assistant/assist", async (HttpContext ctx) =>
        {
            // leaveOpen: true so disposing the reader does not dispose the pipeline-owned request body stream
            using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
            var body = await reader.ReadToEndAsync();

            string? operationType;
            try
            {
                operationType = (string?)JsonNode.Parse(body)?["OperationType"];
            }
            catch (JsonException)
            {
                return Results.BadRequest("Malformed JSON body.");
            }

            if (instance.AssistDelay > TimeSpan.Zero)
                await Task.Delay(instance.AssistDelay, ctx.RequestAborted);

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

        // the appliance posts here after a ConsentRequired, then retries assist; a 200 opens the gate
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
