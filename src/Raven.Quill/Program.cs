using System.Reflection;
using System.Text.Json;
using System.Threading.RateLimiting;
using System.Text.Json.Serialization.Metadata;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using Raven.Quill.Agents;
using Raven.Quill.AiHelper;
using Raven.Quill.Auth;
using Raven.Quill.Endpoints;
using Raven.Quill.Feedback;
using Raven.Quill.Hosting;
using Raven.Quill.Infrastructure;
using Raven.Quill.Licensing;
using Raven.Client.Documents;

var builder = WebApplication.CreateBuilder(args);
var isOpenApiDocumentGeneration = Assembly.GetEntryAssembly()?.GetName().Name == "GetDocument.Insider";

// Kestrel listen URL is needed before IOptions<ApplianceOptions> is resolved;
// read the env directly. The full options object owns the remaining knobs.
var listenUrl = Environment.GetEnvironmentVariable("RAVEN_QUILL_WEB_LISTEN_URL") ?? "http://0.0.0.0:5000";
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
        ReadEnv("RAVEN_QUILL_RAVEN_URL",            v => options.RavenUrl = v);
        ReadEnv("RAVEN_QUILL_WEB_LISTEN_URL",       v => options.WebListenUrl = v);
        ReadEnv("RAVEN_QUILL_CONFIG_DB",            v => options.ConfigDatabase = v);
        ReadEnv("RAVEN_QUILL_SETUP_PACKAGE_PATH",   v => options.SetupPackagePath = v);
        ReadEnv("RAVEN_QUILL_RAVENDB_S6_SERVICE",   v => options.RavenDbS6Service = v);
        ReadEnv("RAVEN_QUILL_LICENSE_API_URL",      v => options.LicenseApiUrl = v);
        ReadEnv("RAVEN_QUILL_API_URL",              v => options.AiApiUrl = v);
        ReadEnv("QUILL_LICENSE_KEY",             v => options.LicenseToken = v);
        ReadEnv("QUILL_API_KEY",                 v => options.ApiKey = v);
        ReadEnv("RAVEN_QUILL_RAVENDB_INTERNAL_PORT", v => { if (int.TryParse(v, out var p)) options.RavenInternalPort = p; });
    })
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IDocumentStore>(sp =>
    RavenStoreFactory.Create(sp.GetRequiredService<IOptions<ApplianceOptions>>().Value));

builder.Services.AddSingleton<IServerReady, ServerReadyFlag>();
builder.Services.AddSingleton<IBootstrapState, BootstrapStateFlag>();
builder.Services.AddSingleton<IAgentRouter, AgentRouter>();
builder.Services.AddSingleton<IApiKeyStore, ApiKeyStore>();
builder.Services.AddSingleton<IFeedbackSender, FeedbackSender>();
// License & Usage stats are served live via the AiHelper proxy: LicenseStatsProvider hits the real
// /license/status, /license-server/connectivity, and /license/quill/usage endpoints on the bundled server.
builder.Services.AddSingleton<ILicenseStatsProvider, LicenseStatsProvider>();
if (!isOpenApiDocumentGeneration)
{
    builder.Services.AddHostedService<RavenReadinessService>();
    // Startup activation (replaces the old POST /api/bootstrap/redeem-license): pulls + unpacks the
    // setup package for QUILL_LICENSE_KEY, then restarts into secure mode (or marks Ready inline).
    builder.Services.AddHostedService<ApplianceActivationService>();
}
builder.Services.AddHttpClient();

// AI Helper client. Always proxies the call through the bundled RavenDB (/assistant/assist), which
// injects the license + cert from its own ServerStore and forwards to api.ravendb.net (test vs prod
// is selected by RAVEN_API_ENV on the bundled server). The bundled server is licensed from the
// activated setup package, so the real AI API is reachable without a separate license-API hop here.
builder.Services.AddHttpClient<IAiHelperClient, AiHelperInternalClient>(static (sp, http) =>
    {
        // The AI-Helper call is proxied through the bundled RavenDB (/assistant/assist), which injects
        // the license + cert and forwards to api.ravendb.net — the appliance never reaches it directly.
        // Default to the store's own node URL (single source of truth for the bundled server); tests
        // override AiApiUrl to point at an in-process mock.
        var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
        var store = sp.GetRequiredService<IDocumentStore>();
        http.BaseAddress = new Uri(string.IsNullOrEmpty(opts.AiApiUrl) ? store.Urls[0] : opts.AiApiUrl);
    })
    .ConfigurePrimaryHttpMessageHandler(static sp =>
    {
        // mTLS to the bundled RavenDB with the admin client cert (as RavenLiveFeedProxy does). The
        // server's wildcard LE cert is OS-trusted, so no custom server-cert validation is needed.
        var store = sp.GetRequiredService<IDocumentStore>();
        var handler = new HttpClientHandler();
        if (store.Certificate is not null)
            handler.ClientCertificates.Add(store.Certificate);
        return handler;
    });

// License client: at startup activation pulls the setup-package zip by token from the real license
// API (RavenDB-26783, GET /api/v1/quill/licenses/{token}), BaseAddress from LicenseApiUrl. The AI
// Helper is separate — it always proxies through the bundled RavenDB via the client registered above.
builder.Services.AddHttpClient<ILicenseClient, LicenseHttpClient>(static (sp, http) =>
{
    var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
    http.BaseAddress = new Uri(opts.LicenseApiUrl);
});

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

// RavenDB-26775 backstop: a coarse per-IP cap on the public embed chat route.
// The minted link's invocation cap + TTL is the PRIMARY control; this only
// blunts token-brute-forcing the 410/404 path and high-N "public" tokens.
// Behind the nginx :443 front the client IP is the loopback proxy (the TLS-passthrough SNI
// listener can't carry the real IP), so this partitions per-appliance; the minted link's
// invocation cap + TTL remains the primary control regardless.
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddPolicy(EmbedEndpoints.ChatRateLimitPolicy, httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            // Fall back to the per-connection id (not a shared "unknown" bucket) when
            // the remote IP is unavailable — e.g. in-memory TestServer — so requests
            // aren't all collapsed into one partition.
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 60,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
            }));

    // Brute-force backstop on the operator login. The API key is high-entropy (unguessable), so this
    // mainly bounds online guessing of an operator-chosen QUILL_API_KEY. Behind the nginx :443 front the
    // partition key is the loopback proxy IP, so this is effectively a global ~10/min login cap.
    options.AddPolicy(AuthEndpoints.LoginRateLimitPolicy, httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 10,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
            }));
});

// Operator authentication: an API-key header (api.*) or a login-issued session cookie (dashboard.*),
// both validated against IApiKeyStore. Admin endpoints require either credential; the public surfaces
// (auth, bootstrap status, /healthz, static assets, /embed/*) stay anonymous.
builder.Services
    .AddAuthentication(options =>
    {
        options.DefaultScheme = ApiKeyAuthenticationHandler.SchemeName;
        options.DefaultSignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    })
    .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(ApiKeyAuthenticationHandler.SchemeName, null)
    .AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, options =>
    {
        options.Cookie.Name = "quill.session";
        options.Cookie.HttpOnly = true;
        // SameAsRequest while the appliance is on plain HTTP behind the demo; the Phase-1 TLS front
        // plus UseForwardedHeaders make this effectively Always (Secure) in production.
        options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.Cookie.SameSite = SameSiteMode.Strict;
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
        // API surface: answer 401/403 instead of redirecting to a login page.
        options.Events.OnRedirectToLogin = ctx =>
        {
            ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return Task.CompletedTask;
        };
        options.Events.OnRedirectToAccessDenied = ctx =>
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            return Task.CompletedTask;
        };
    });

builder.Services.AddAuthorization(options =>
{
    options.DefaultPolicy = new AuthorizationPolicyBuilder(
            ApiKeyAuthenticationHandler.SchemeName, CookieAuthenticationDefaults.AuthenticationScheme)
        .RequireAuthenticatedUser()
        .Build();
});

// Behind the in-container nginx :443 SNI front: honor the forwarded scheme/host so the session cookie
// goes Secure and embed-link URLs use https + the request host. The only proxy is nginx on loopback —
// trust it explicitly (the defaults trust just ::1 and would otherwise drop the forwarded values).
// Note: the TLS-passthrough SNI listener can't carry the real client IP, so X-Forwarded-For is the
// loopback proxy and the rate limiter buckets per-appliance, not per-client.
builder.Services.Configure<ForwardedHeadersOptions>(options =>
{
    options.ForwardedHeaders =
        ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto | ForwardedHeaders.XForwardedHost;
    options.KnownProxies.Clear();
    options.KnownIPNetworks.Clear();
    options.KnownProxies.Add(System.Net.IPAddress.Loopback);
    options.KnownProxies.Add(System.Net.IPAddress.IPv6Loopback);
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

// Dev-mode safeguard: with a QUILL_LICENSE_KEY set but no RAVEN_QUILL_LICENSE_API_URL override, a local
// run hits the real api.ravendb.net during startup activation and hangs (no test license to redeem);
// with no token, activation is skipped and nothing is hit. Warn loudly at startup; in Production we trust the default.
{
    var opts = app.Services.GetRequiredService<IOptions<ApplianceOptions>>().Value;
    if (app.Environment.IsDevelopment() &&
        string.Equals(opts.LicenseApiUrl, ApplianceOptions.DefaultLicenseApiUrl, StringComparison.OrdinalIgnoreCase))
    {
        app.Logger.LogWarning(
            "LicenseApiUrl is set to the production default ({Default}); set RAVEN_QUILL_LICENSE_API_URL to a mock or staging endpoint for local development.",
            ApplianceOptions.DefaultLicenseApiUrl);
    }
}

// Must run first: rewrites Request.Scheme/Host + RemoteIpAddress from the nginx :443 front's
// X-Forwarded-* before anything reads them (cookie Secure policy, embed URLs, rate-limit partition).
app.UseForwardedHeaders();

// Enables WebSocket upgrades for the live-feed proxy (e.g. /api/apps/{slug}/cdc/progress).
app.UseWebSockets();

app.UseReadinessGate();
app.UseRateLimiter();
app.UseAuthentication();
app.UseAuthorization();

StaticAssetEndpoints.Map(app);
HealthEndpoints.Map(app);
BootstrapEndpoints.Map(app);
AuthEndpoints.Map(app);
AppsEndpoints.Map(app);
ChannelsEndpoints.Map(app);
IFrameCustomizationEndpoints.Map(app);
EmbedLinksEndpoints.Map(app);
AiConnectionStringsEndpoints.Map(app);
AiModelsEndpoints.Map(app);
AgentsEndpoints.Map(app);
StatsEndpoints.Map(app);
SettingsEndpoints.Map(app);
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
