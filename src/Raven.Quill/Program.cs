using System.Reflection;
using System.Text.Json;
using System.Threading.RateLimiting;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly;
using Raven.Quill.Agents;
using Raven.Quill.AiHelper;
using Raven.Quill.Auth;
using Raven.Quill.Embed;
using Raven.Quill.Endpoints;
using Raven.Quill.Feedback;
using Raven.Quill.Hosting;
using Raven.Quill.Infrastructure;
using Raven.Quill.Licensing;
using Raven.Quill.Discord;
using Raven.Quill.Slack;
using Raven.Quill.Telegram;
using Raven.Client.Documents;

var builder = WebApplication.CreateBuilder(args);
var isOpenApiDocumentGeneration = Assembly.GetEntryAssembly()?.GetName().Name == "GetDocument.Insider";

// loopback by default; the container sets the bind via RAVEN_QUILL_WEB_LISTEN_URL
var listenUrl = Environment.GetEnvironmentVariable("RAVEN_QUILL_WEB_LISTEN_URL") ?? "http://127.0.0.1:5000";
builder.WebHost.UseUrls(listenUrl);

// enums as string names so operators can paste Studio JSON
builder.Services.ConfigureHttpJsonOptions(static options =>
{
    // message role lowercased ("assistant"/"user") for the FE + embed-widget contract; other enums stay PascalCase
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter<Raven.Client.Documents.Operations.AI.Agents.AiMessageRole>(JsonNamingPolicy.CamelCase));
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

builder.Logging.AddFilter("Polly", LogLevel.None);

builder.Services.AddOptions<ApplianceOptions>()
    .Configure(options =>
    {
        ReadEnv("RAVEN_QUILL_RAVEN_URL", v => options.RavenUrl = v);
        ReadEnv("RAVEN_QUILL_WEB_LISTEN_URL", v => options.WebListenUrl = v);
        ReadEnv("RAVEN_QUILL_CONFIG_DB", v => options.ConfigDatabase = v);
        ReadEnv("RAVEN_QUILL_SETUP_PACKAGE_PATH", v => options.SetupPackagePath = v);
        ReadEnv("RAVEN_QUILL_RAVENDB_S6_SERVICE", v => options.RavenDbS6Service = v);
        ReadEnv("RAVEN_QUILL_API_URL", v => options.AiApiUrl = v);
        ReadEnv("RAVEN_QUILL_TELEGRAM_API_URL", v => options.Telegram.ApiUrl = v);
        ReadEnv("RAVEN_QUILL_SLACK_API_URL", v => options.Slack.ApiUrl = v);
        ReadEnv("RAVEN_QUILL_DISCORD_API_URL", v => options.Discord.ApiUrl = v);
        ReadEnv("QUILL_LICENSE_KEY", v => options.LicenseKey = v);
        ReadEnv("QUILL_API_KEY", v => options.ApiKey = v);
        ReadEnv("RAVEN_QUILL_RAVENDB_INTERNAL_PORT", v =>
        {
            if (int.TryParse(v, out var p)) options.RavenInternalPort = p;
        });
        ReadEnv("RAVEN_QUILL_AI_ASSIST_TIMEOUT_SECONDS", v =>
        {
            if (int.TryParse(v, out var s) && s > 0) options.AiAssistTimeout = TimeSpan.FromSeconds(s);
        });
        ReadEnv("RAVEN_QUILL_READINESS_INITIAL_DELAY_SECONDS", v =>
            options.ReadinessInitialDelay = ParsePositiveSeconds("RAVEN_QUILL_READINESS_INITIAL_DELAY_SECONDS", v));
        ReadEnv("RAVEN_QUILL_READINESS_ATTEMPT_TIMEOUT_SECONDS", v =>
            options.ReadinessAttemptTimeout = ParsePositiveSeconds("RAVEN_QUILL_READINESS_ATTEMPT_TIMEOUT_SECONDS", v));
        ReadEnv("RAVEN_QUILL_READINESS_OVERALL_TIMEOUT_SECONDS", v =>
            options.ReadinessOverallTimeout = ParsePositiveSeconds("RAVEN_QUILL_READINESS_OVERALL_TIMEOUT_SECONDS", v));
    })
    .ValidateDataAnnotations()
    .Validate(o => string.IsNullOrEmpty(o.Telegram.ApiUrl) ||
                   Uri.TryCreate(o.Telegram.ApiUrl, UriKind.Absolute, out var u) &&
                   (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
        "Telegram ApiUrl must be an absolute http(s) URL")
    .Validate(o => o.Telegram.MessageLimit is > 0 and <= TelegramOptions.ApiMessageLimit,
        $"Telegram MessageLimit must be between 1 and {TelegramOptions.ApiMessageLimit}")
    .Validate(o => o.Telegram.ChatQueueCapacity > 0, "Telegram ChatQueueCapacity must be positive")
    .Validate(o => o.Telegram.EditDebounce > TimeSpan.Zero, "Telegram EditDebounce must be positive")
    .Validate(o => o.Telegram.ApplyChangesInterval > TimeSpan.Zero, "Telegram ApplyChangesInterval must be positive")
    .Validate(o => o.Telegram.ChatIdleTimeout > TimeSpan.Zero, "Telegram ChatIdleTimeout must be positive")
    .Validate(o => o.Telegram.PollBackoffMax > TimeSpan.Zero, "Telegram PollBackoffMax must be positive")
    .Validate(o => Uri.TryCreate(o.Slack.ApiUrl, UriKind.Absolute, out var u) &&
                   (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
        "Slack ApiUrl must be an absolute http(s) URL")
    .Validate(o => o.Slack.RequestTimeout > TimeSpan.Zero, "Slack RequestTimeout must be positive")
    .Validate(o => o.Slack.MaxWebhookBodyBytes > 0, "Slack MaxWebhookBodyBytes must be positive")
    .Validate(o => o.Slack.MessageLimit is > 0 and <= SlackOptions.ApiMessageLimit / SlackMrkdwn.MaxEscapeExpansion,
        $"Slack MessageLimit must be between 1 and {SlackOptions.ApiMessageLimit / SlackMrkdwn.MaxEscapeExpansion}, " +
        "so the worst-case mrkdwn escape stays within Slack's message cap")
    .Validate(o => o.Slack.EditDebounce > TimeSpan.Zero, "Slack EditDebounce must be positive")
    .Validate(o => o.Slack.SenderQueueCapacity > 0, "Slack SenderQueueCapacity must be positive")
    .Validate(o => o.Slack.SignatureTolerance > TimeSpan.Zero, "Slack SignatureTolerance must be positive")
    .Validate(o => Uri.TryCreate(o.Discord.ApiUrl, UriKind.Absolute, out var u) &&
                   (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
        "Discord ApiUrl must be an absolute http(s) URL")
    .Validate(o => o.Discord.RequestTimeout > TimeSpan.Zero, "Discord RequestTimeout must be positive")
    .Validate(o => o.Discord.MessageLimit is > 0 and <= DiscordOptions.ApiMessageLimit,
        $"Discord MessageLimit must be between 1 and {DiscordOptions.ApiMessageLimit}")
    .Validate(o => o.Discord.EditDebounce > TimeSpan.Zero, "Discord EditDebounce must be positive")
    .Validate(o => o.Discord.SenderQueueCapacity > 0, "Discord SenderQueueCapacity must be positive")
    .Validate(o => o.Discord.ApplyChangesInterval > TimeSpan.Zero, "Discord ApplyChangesInterval must be positive")
    .Validate(o => o.Discord.GatewayBackoffMax > TimeSpan.Zero, "Discord GatewayBackoffMax must be positive")
    .Validate(o => o.Discord.GatewayHandshakeTimeout > TimeSpan.Zero,
        "Discord GatewayHandshakeTimeout must be positive")
    .Validate(o => o.Discord.GatewayRestartDelay > TimeSpan.Zero, "Discord GatewayRestartDelay must be positive")
    .Validate(o => o.Discord.MaxGatewayFrameBytes > 0, "Discord MaxGatewayFrameBytes must be positive")
    .ValidateOnStart();

builder.Services.AddSingleton<IDocumentStore>(sp =>
    RavenStoreFactory.Create(sp.GetRequiredService<IOptions<ApplianceOptions>>().Value));

builder.Services.AddSingleton<IServerReady, ServerReadyFlag>();
builder.Services.AddSingleton<IQuillExpiry>(_ => new QuillExpiry(DateTime.UtcNow));
// Resolved only by an expired build's gate, so a live start never reads the file.
builder.Services.AddSingleton(sp => ExpiryNotice.Load(sp.GetRequiredService<IWebHostEnvironment>()));
builder.Services.AddSingleton<IBootstrapState, BootstrapStateFlag>();
builder.Services.AddSingleton<IAgentRouter, AgentRouter>();
builder.Services.AddSingleton<WebhookActionExecutor>();
builder.Services.AddSingleton<IApiKeyStore, ApiKeyStore>();

// Read once at startup: the widget manifest only changes when the image is rebuilt, and a missing one is
// worth logging loudly the moment the process starts rather than on the first visitor's request.
builder.Services.AddSingleton(sp => WidgetAssets.Load(
    sp.GetRequiredService<IWebHostEnvironment>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<WidgetAssets>()));
builder.Services.AddTransient<IFeedbackSender, FeedbackSender>();
builder.Services.AddTransient<ILicenseStatsProvider, LicenseStatsProvider>();
builder.Services.AddSingleton<ITelegramBotClientFactory, TelegramBotClientFactory>();
builder.Services.AddSingleton<SlackHealthRegistry>();
builder.Services.AddSingleton<SlackUserDirectory>();
builder.Services.AddSingleton<SlackInboundProcessor>();
builder.Services.AddSingleton<DiscordHealthRegistry>();
builder.Services.AddSingleton<DiscordInboundProcessor>();
builder.Services.AddSingleton<DiscordChannelManager>();
builder.Services.AddSingleton<IDiscordChannelManager>(sp => sp.GetRequiredService<DiscordChannelManager>());
builder.Services.AddSingleton<TelegramChannelManager>();
builder.Services.AddSingleton<ITelegramChannelManager>(sp => sp.GetRequiredService<TelegramChannelManager>());
if (!isOpenApiDocumentGeneration)
{
    builder.Services.AddHostedService<RavenReadinessService>();
    builder.Services.AddHostedService<ApplianceActivationService>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<TelegramChannelManager>());
    builder.Services.AddHostedService(sp => sp.GetRequiredService<SlackInboundProcessor>());
    builder.Services.AddHostedService(sp => sp.GetRequiredService<DiscordInboundProcessor>());
    builder.Services.AddHostedService(sp => sp.GetRequiredService<DiscordChannelManager>());
}

builder.Services.ConfigureHttpClientDefaults(httpBuilder =>
{
    httpBuilder.ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
    {
        AllowAutoRedirect = false
    });
});

builder.Services.AddHttpClient(WebhookActionExecutor.ClientName,
    static http => http.Timeout = TimeSpan.FromSeconds(30));

builder.Services.AddHttpClient<ISlackClient, SlackApiClient>(static (sp, http) =>
{
    var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value.Slack;
    http.BaseAddress = new Uri(opts.ApiUrl.EndsWith('/') ? opts.ApiUrl : opts.ApiUrl + "/");
    http.Timeout = opts.RequestTimeout;
});

builder.Services.AddHttpClient<IDiscordClient, DiscordApiClient>(static (sp, http) =>
{
    var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value.Discord;
    http.BaseAddress = new Uri(opts.ApiUrl.EndsWith('/') ? opts.ApiUrl : opts.ApiUrl + "/");
    http.Timeout = opts.RequestTimeout;
});

builder.Services.AddHttpClient<IAiHelperClient, AiHelperInternalClient>(static (sp, http) =>
    {
        var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
        var store = sp.GetRequiredService<IDocumentStore>();
        http.BaseAddress = new Uri(string.IsNullOrEmpty(opts.AiApiUrl) ? store.Urls[0] : opts.AiApiUrl);
        http.Timeout = opts.AiAssistTimeout;
    })
    .ConfigurePrimaryHttpMessageHandler(static sp =>
    {
        var store = sp.GetRequiredService<IDocumentStore>();
        var handler = new HttpClientHandler
        {
            AllowAutoRedirect = false
        };
        if (store.Certificate is not null)
            handler.ClientCertificates.Add(store.Certificate);
        return handler;
    });

builder.Services.AddSingleton<ILicenseClient, LicenseHttpClient>();

builder.Services.ConfigureHttpJsonOptions(opts =>
{
    opts.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter());
});

builder.Services.AddResiliencePipeline(RavenReadinessService.PipelineName, (pipelineBuilder, ctx) =>
{
    var opts = ctx.ServiceProvider.GetRequiredService<IOptions<ApplianceOptions>>().Value;
    RavenReadinessService.ConfigureProbePipeline(pipelineBuilder, opts);
});

builder.Services.AddHealthChecks()
    .AddCheck<RavenHealthCheck>("ravendb", failureStatus: HealthStatus.Unhealthy);

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    // coarse per-IP backstop; the link's invocation cap + TTL is the primary control
    options.AddPolicy(EmbedEndpoints.ChatRateLimitPolicy, httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 60,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
            }));

    options.AddPolicy(SlackEndpoints.WebhookRateLimitPolicy, httpContext =>
        RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 600,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
            }));


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
        options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.Cookie.SameSite = SameSiteMode.Strict;
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
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

// trust only the nginx loopback proxy so forwarded scheme/host/IP are honored
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

app.UseForwardedHeaders();

app.UseExpiryGate();

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
SlackEndpoints.Map(app);
DiscordEndpoints.Map(app);
IFrameCustomizationEndpoints.Map(app);
EmbedLinksEndpoints.Map(app);
AiConnectionStringsEndpoints.Map(app);
AiModelsEndpoints.Map(app);
AgentsEndpoints.Map(app);
StatsEndpoints.Map(app);
SettingsEndpoints.Map(app);
WizardEndpoints.Map(app);
ChatEndpoints.Map(app);
AssistantEndpoints.Map(app);
// map before MapSpaFallback or /apps/{slug}/embed/* is swallowed as index.html
EmbedEndpoints.Map(app);
StaticAssetEndpoints.MapSpaFallback(app);

app.Run();

return;

static void ReadEnv(string name, Action<string> apply)
{
    var v = Environment.GetEnvironmentVariable(name);
    if (!string.IsNullOrEmpty(v)) apply(v);
}

static TimeSpan ParsePositiveSeconds(string name, string value)
{
    if (int.TryParse(value, out var seconds) == false || seconds <= 0)
        throw new InvalidOperationException($"{name} must be a positive number of seconds, got '{value}'");
    return TimeSpan.FromSeconds(seconds);
}

static string GetJsonPropertyName(System.Reflection.PropertyInfo property)
{
    var attribute = property.GetCustomAttribute<JsonPropertyNameAttribute>();
    if (attribute is not null)
        return attribute.Name;

    return JsonNamingPolicy.CamelCase.ConvertName(property.Name);
}

public partial class Program;
