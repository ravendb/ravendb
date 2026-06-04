using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using Raven.AiAppliance.Agents;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Endpoints;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Schema.Demo;
using Raven.Client.Documents;

var builder = WebApplication.CreateBuilder(args);
var isOpenApiDocumentGeneration = Assembly.GetEntryAssembly()?.GetName().Name == "GetDocument.Insider";

// Kestrel listen URL is needed before IOptions<ApplianceOptions> is resolved;
// read the env directly. The full options object owns the remaining knobs.
var listenUrl = Environment.GetEnvironmentVariable("RAVEN_AI_WEB_LISTEN_URL") ?? "http://0.0.0.0:5000";
builder.WebHost.UseUrls(listenUrl);

builder.Services.ConfigureHttpJsonOptions(static options =>
{
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

builder.Services.AddOpenApi(options =>
{
    options.AddSchemaTransformer((schema, context, _) =>
    {
        if (schema.Properties is null || schema.Properties.Count == 0)
            return Task.CompletedTask;

        var writableProperties = context.JsonTypeInfo.Type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(static property => property.GetMethod is { IsPublic: true } && property.SetMethod is { IsPublic: true })
            .Select(GetJsonPropertyName)
            .ToHashSet(StringComparer.Ordinal);

        var getterOnlyProperties = schema.Properties.Keys
            .Where(propertyName => writableProperties.Contains(propertyName) == false)
            .ToArray();

        foreach (var propertyName in getterOnlyProperties)
        {
            schema.Properties.Remove(propertyName);
            schema.Required?.Remove(propertyName);
        }

        return Task.CompletedTask;
    });
});

// Silence Polly's framework-level retry telemetry. During RavenDB startup the
// readiness probe retries 3-5x while the server boots; without this filter,
// each failure dumps a full stack trace at warn level. Our RavenReadinessService
// still logs the success line (and a single failure line if the overall timeout
// fires) at info / error.
builder.Logging.AddFilter("Polly", LogLevel.None);

builder.Services.AddOptions<ApplianceOptions>()
    .Configure(options =>
    {
        ReadEnv("RAVEN_AI_RAVEN_URL",            v => options.RavenUrl = v);
        ReadEnv("RAVEN_AI_WEB_LISTEN_URL",       v => options.WebListenUrl = v);
        ReadEnv("RAVEN_AI_CONFIG_DB",            v => options.ConfigDatabase = v);
        ReadEnv("RAVEN_AI_SETUP_PACKAGE_PATH",   v => options.SetupPackagePath = v);
        ReadEnv("RAVEN_AI_SETUP_PACKAGE_ZIP",    v => options.SetupPackageZipPath = v);
        ReadEnv("RAVEN_AI_RAVENDB_S6_SERVICE",   v => options.RavenDbS6Service = v);
        ReadEnv("RAVEN_AI_LICENSE_API_URL",      v => options.LicenseApiUrl = v);
        ReadEnv("RAVEN_AI_API_URL",              v => options.AiApiUrl = v);
    })
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IDocumentStore>(sp =>
    RavenStoreFactory.Create(sp.GetRequiredService<IOptions<ApplianceOptions>>().Value));

builder.Services.AddSingleton<IServerReady, ServerReadyFlag>();
builder.Services.AddSingleton<IBootstrapState, BootstrapStateFlag>();
builder.Services.AddSingleton<IAgentSchemaRegistry, AgentSchemaRegistry>();
builder.Services.AddSingleton<IAgentSchema, DemoAgentSchema>();
builder.Services.AddSingleton<IAgentRouter, AgentRouter>();
// Embed conversation tokens (RavenDB-26700 auth follow-up). TimeProvider is
// injected (not DateTime.UtcNow) so binding-expiry logic is testable.
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<ConversationBindings>();
if (!isOpenApiDocumentGeneration)
    builder.Services.AddHostedService<RavenReadinessService>();
builder.Services.AddHttpClient();

// AI Helper: identity provider (license.json + admin-thumbprint) and the AI-Helper
// client. In demo mode, the same local setup-package zip that makes the bootstrap
// endpoint bypass the real license API (see BootstrapEndpoints) also leaves the
// internal AI service on api.ravendb.net unreachable, so serve canned Northwind sample
// data via MockAiHelperClient. Production (no zip) always uses the real HTTP client
// dialed at ApplianceOptions.AiApiUrl. Read the env directly here, like listenUrl above:
// IOptions isn't resolved yet at registration time.
builder.Services.AddSingleton<IApplianceLicenseProvider, SetupPackageLicenseProvider>();

var setupPackageZip = Environment.GetEnvironmentVariable("RAVEN_AI_SETUP_PACKAGE_ZIP");
var useAiHelperMock = string.IsNullOrEmpty(setupPackageZip) == false && File.Exists(setupPackageZip);
if (useAiHelperMock)
{
    builder.Services.AddSingleton<IAiHelperClient, MockAiHelperClient>();
}
else
{
    builder.Services.AddHttpClient<IAiHelperClient, AiHelperInternalClient>(static (sp, http) =>
    {
        var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
        http.BaseAddress = new Uri(opts.AiApiUrl);
    });
}

// Wire-shape: enums travel as their string names (e.g. AiModelType "Chat" not 1,
// AiConnectorType "Ollama" not 3). Matches RavenDB Studio's payload, lets
// operators paste the same JSON they'd paste into the AI tab.
builder.Services.ConfigureHttpJsonOptions(opts =>
{
    opts.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter());
});

builder.Services.AddResiliencePipeline(RavenReadinessService.PipelineName, (pipelineBuilder, ctx) =>
{
    var opts = ctx.ServiceProvider.GetRequiredService<IOptions<ApplianceOptions>>().Value;
    pipelineBuilder
        // Innermost: per-attempt timeout. Polly's TimeoutStrategy raises
        // TimeoutRejectedException (not OperationCanceledException), so the
        // retry above will treat a slow probe as retryable instead of giving up.
        .AddTimeout(new TimeoutStrategyOptions { Timeout = opts.ReadinessAttemptTimeout })
        // Middle: retry transient errors and per-attempt timeouts. Excludes
        // OperationCanceledException — that only flows when the outer
        // stoppingToken is cancelled (host shutdown), where retrying is wrong.
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            Delay = TimeSpan.FromMilliseconds(250),
            MaxDelay = TimeSpan.FromSeconds(2),
            MaxRetryAttempts = int.MaxValue,
        })
        // Outermost: overall budget across all retry attempts.
        .AddTimeout(new TimeoutStrategyOptions { Timeout = opts.ReadinessOverallTimeout });
});

builder.Services.AddHealthChecks()
    .AddCheck<RavenHealthCheck>("ravendb", failureStatus: HealthStatus.Unhealthy);

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

// Dev-mode safeguard: a forgetful local run with no RAVEN_AI_LICENSE_API_URL
// override will hit the real api.ravendb.net on first /api/bootstrap/redeem-license
// and hang (no test license to redeem). Warn loudly at startup so the operator
// notices before triggering activation; in Production we trust the default.
{
    var opts = app.Services.GetRequiredService<IOptions<ApplianceOptions>>().Value;
    if (app.Environment.IsDevelopment() &&
        string.Equals(opts.LicenseApiUrl, ApplianceOptions.DefaultLicenseApiUrl, StringComparison.OrdinalIgnoreCase))
    {
        app.Logger.LogWarning(
            "LicenseApiUrl is set to the production default ({Default}); set RAVEN_AI_LICENSE_API_URL to a mock or staging endpoint for local development.",
            ApplianceOptions.DefaultLicenseApiUrl);
    }

    if (useAiHelperMock)
    {
        app.Logger.LogInformation(
            "AI Helper is running in demo mode: suggest/cdc and suggest/agent return canned Northwind sample data (MockAiHelperClient), not live results from the internal AI service.");
    }
}

// Enables WebSocket upgrades for the live-feed proxy (e.g. /api/apps/{slug}/cdc/progress).
app.UseWebSockets();

app.UseReadinessGate();

StaticAssetEndpoints.Map(app);
HealthEndpoints.Map(app);
BootstrapEndpoints.Map(app);
AppsEndpoints.Map(app);
ChannelsEndpoints.Map(app);
AiConnectionStringsEndpoints.Map(app);
WizardEndpoints.Map(app);
ChatEndpoints.Map(app);
// Must precede MapSpaFallback: /embed/* is a {*path:nonfile} match that the
// SPA fallback would otherwise swallow and serve index.html instead.
EmbedEndpoints.Map(app);
StaticAssetEndpoints.MapSpaFallback(app);

app.Run();

return;

static void ReadEnv(string name, Action<string> apply)
{
    var v = Environment.GetEnvironmentVariable(name);
    if (!string.IsNullOrEmpty(v)) apply(v);
}

static string GetJsonPropertyName(System.Reflection.PropertyInfo property)
{
    var attribute = property.GetCustomAttribute<JsonPropertyNameAttribute>();
    if (attribute is not null)
        return attribute.Name;

    return JsonNamingPolicy.CamelCase.ConvertName(property.Name);
}

// Make Program reachable for WebApplicationFactory<Program> in the test project.
public partial class Program;
