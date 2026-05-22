using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using Raven.AiAppliance.Endpoints;
using Raven.AiAppliance.Hosting;
using Raven.AiAppliance.Infrastructure;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Schema.Demo;
using Raven.Client.Documents;

var builder = WebApplication.CreateBuilder(args);

// Kestrel listen URL is needed before IOptions<ApplianceOptions> is resolved;
// read the env directly. The full options object owns the remaining knobs.
var listenUrl = Environment.GetEnvironmentVariable("RAVEN_AI_WEB_LISTEN_URL") ?? "http://0.0.0.0:5000";
builder.WebHost.UseUrls(listenUrl);

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
        ReadEnv("RAVEN_AI_LICENSE_API_URL",      v => options.LicenseApiUrl = v);
        ReadEnv("RAVEN_AI_LLM_PROVIDER",         v => options.LlmProvider = v);
        ReadEnv("RAVEN_AI_LLM_ENDPOINT",         v => options.LlmEndpoint = v);
        ReadEnv("RAVEN_AI_LLM_MODEL",            v => options.LlmModel = v);
        ReadEnv("RAVEN_AI_LLM_API_KEY",          v => options.LlmApiKey = v);
    })
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IDocumentStore>(sp =>
    RavenStoreFactory.Create(sp.GetRequiredService<IOptions<ApplianceOptions>>().Value));

builder.Services.AddSingleton<IServerReady, ServerReadyFlag>();
builder.Services.AddSingleton<IBootstrapState, BootstrapStateFlag>();
builder.Services.AddSingleton<IAgentSchemaRegistry, AgentSchemaRegistry>();
builder.Services.AddSingleton<IAgentSchema, DemoAgentSchema>();
builder.Services.AddHostedService<RavenReadinessService>();
builder.Services.AddHttpClient();

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
}

StaticAssetEndpoints.Map(app);
HealthEndpoints.Map(app);
BootstrapEndpoints.Map(app);
AppsEndpoints.Map(app);
WizardEndpoints.Map(app);
ChatEndpoints.Map(app);
StaticAssetEndpoints.MapSpaFallback(app);

app.Run();

return;

static void ReadEnv(string name, Action<string> apply)
{
    var v = Environment.GetEnvironmentVariable(name);
    if (!string.IsNullOrEmpty(v)) apply(v);
}

// Make Program reachable for WebApplicationFactory<Program> in the test project.
public partial class Program;
