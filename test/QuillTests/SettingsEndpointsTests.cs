using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Feedback;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the settings surfaces — <c>GET /api/settings/license</c> and
/// <c>GET /api/settings/usage</c>. Both are RavenDB-backed (see
/// <c>LicenseStatsProvider</c>): license proxies the server's <c>/license/status</c> +
/// <c>/license-server/connectivity</c> and appends the static plan catalog; usage
/// proxies <c>/license/quill/usage</c>. Assertions target the response shape and
/// environment-stable fields, not license-specific values (which vary with whatever
/// license the test server runs under).
/// </summary>
public class SettingsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task License_surfaces_server_license_connectivity_and_plans()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var license = await client.GetFromJsonAsync<JsonElement>("/api/settings/license");

        // response: the server's /license/status, projected onto ServerLicenseResponse.
        var response = license.GetProperty("response");
        Assert.False(string.IsNullOrEmpty(response.GetProperty("status").GetString()));  // e.g. "Commercial" / "AGPL - Open Source"
        Assert.False(string.IsNullOrEmpty(response.GetProperty("type").GetString()));     // e.g. "EnterpriseAi" / "None"
        Assert.True(response.GetProperty("expired").ValueKind is JsonValueKind.True or JsonValueKind.False);

        // connectivity: the server's /license-server/connectivity probe.
        Assert.False(string.IsNullOrEmpty(license.GetProperty("connectivity").GetProperty("statusCode").GetString()));

        // plans: the static catalog LicenseStatsProvider always appends.
        var plans = license.GetProperty("plans");
        Assert.True(plans.GetArrayLength() >= 1);
        Assert.Equal("enterprise", plans[0].GetProperty("slug").GetString());
        Assert.True(plans[0].GetProperty("features").GetArrayLength() >= 1);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_returns_quill_usage_payload()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/settings/usage?year=2026&month=5");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var usage = await resp.Content.ReadFromJsonAsync<JsonElement>();
        // QuillUsageResponse { perApplication, byPeriod } — both may be null when the
        // server reports no usage, but the shape must always be present.
        Assert.True(usage.TryGetProperty("perApplication", out _));
        Assert.True(usage.TryGetProperty("byPeriod", out _));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_forwards_month_without_client_side_validation()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // The endpoint forwards year/month straight to RavenDB's /license/quill/usage;
        // it does not reject out-of-range months itself (contrast the former mock, which
        // 400'd on month=13). Characterizes current behavior — see note if validation
        // should move back into the appliance.
        var resp = await client.GetAsync("/api/settings/usage?year=2026&month=13");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_accepts_only_user_fields_and_normalizes_them()
    {
        var sender = new RecordingFeedbackSender();
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.UserAgent.ParseAdd("Quill-Test/1.0");

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = "  Jane Doe ",
            Email = "  user@example.com ",
            Impression = " POSITIVE ",
            Message = "  Please contact me. ",
            StudioView = " /dashboard/license ",
        });

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(sender.Request);
        Assert.Equal("Jane Doe", sender.Request.Name);
        Assert.Equal("user@example.com", sender.Request.Email);
        Assert.Equal("positive", sender.Request.Impression);
        Assert.Equal("Please contact me.", sender.Request.Message);
        Assert.Equal("/dashboard/license", sender.Request.StudioView);
        Assert.Contains("Quill-Test/1.0", sender.UserAgent);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_treats_impression_and_studio_view_as_optional()
    {
        var sender = new RecordingFeedbackSender();
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = "Jane Doe",
            Email = "user@example.com",
            Impression = " ",
            Message = "Just a question.",
        });

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(sender.Request);
        Assert.Null(sender.Request.Impression);
        Assert.Null(sender.Request.StudioView);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(" ", "user@example.com", "positive", "Message.")]
    [InlineData("Jane Doe", "not-an-email", "positive", "Message.")]
    [InlineData("Jane Doe", "user@example.com", "meh", "Message.")]
    [InlineData("Jane Doe", "user@example.com", "positive", " ")]
    public async Task Feedback_rejects_invalid_input(string name, string email, string impression, string message)
    {
        var sender = new RecordingFeedbackSender();
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = name,
            Email = email,
            Impression = impression,
            Message = message,
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(sender.Request);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(257, 254, 8_192, 512)]   // name over its cap
    [InlineData(256, 255, 8_192, 512)]   // email over its cap
    [InlineData(256, 254, 8_193, 512)]   // message over its cap
    [InlineData(256, 254, 8_192, 513)]   // studio view over its cap
    public async Task Feedback_rejects_over_long_fields(int nameLength, int emailLength, int messageLength, int studioViewLength)
    {
        var sender = new RecordingFeedbackSender();
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = new string('n', nameLength),
            Email = "u@" + new string('d', emailLength - 6) + ".com",
            Impression = "positive",
            Message = new string('m', messageLength),
            StudioView = new string('v', studioViewLength),
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(sender.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_accepts_fields_at_their_length_caps()
    {
        var sender = new RecordingFeedbackSender();
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = new string('n', 256),
            Email = "u@" + new string('d', 254 - 6) + ".com",
            Impression = "positive",
            Message = new string('m', 8_192),
            StudioView = new string('v', 512),
        });

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(sender.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_returns_bad_gateway_when_sending_fails()
    {
        var sender = new RecordingFeedbackSender(sendResult: false);
        using var factory = NewFeedbackFactory(sender);
        var client = factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = "Jane Doe",
            Email = "user@example.com",
            Impression = "negative",
            Message = "Something went wrong.",
        });

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_sender_builds_the_ravendb_feedback_contract()
    {
        var client = new RecordingAiHelperClient();
        var sender = new FeedbackSender(client);

        bool wasSent = await sender.SendAsync(
            new SendFeedbackRequest("Jane Doe", "user@example.com", "negative", "Something went wrong.", "/dashboard/license"),
            "Quill-Test/1.0",
            CancellationToken.None);

        Assert.True(wasSent);
        Assert.Equal("/studio/feedback", client.Path);
        Assert.Equal("POST", client.Method);

        JsonElement payload = JsonSerializer.SerializeToElement(client.Request);
        Assert.Equal("Something went wrong.", payload.GetProperty("Message").GetString());

        JsonElement product = payload.GetProperty("Product");
        Assert.Equal("RavenDB", product.GetProperty("Name").GetString());
        Assert.False(string.IsNullOrWhiteSpace(product.GetProperty("Version").GetString()));
        Assert.False(string.IsNullOrWhiteSpace(product.GetProperty("StudioVersion").GetString()));
        Assert.Equal("/dashboard/license", product.GetProperty("StudioView").GetString());
        Assert.Equal("Quill", product.GetProperty("FeatureName").GetString());
        Assert.Equal("negative", product.GetProperty("FeatureImpression").GetString());

        JsonElement user = payload.GetProperty("User");
        Assert.Equal("Jane Doe", user.GetProperty("Name").GetString());
        Assert.Equal("user@example.com", user.GetProperty("Email").GetString());
        Assert.Equal("Quill-Test/1.0", user.GetProperty("UserAgent").GetString());
    }

    private WebApplicationFactory<Program> NewFeedbackFactory(IFeedbackSender sender)
    {
        var store = GetDocumentStore();
        var baseFactory = NewApplianceFactory(store);
        return baseFactory.WithWebHostBuilder(builder =>
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IFeedbackSender>();
                services.AddSingleton(sender);
            }));
    }

    private sealed class RecordingFeedbackSender(bool sendResult = true) : IFeedbackSender
    {
        public SendFeedbackRequest? Request { get; private set; }
        public string? UserAgent { get; private set; }

        public Task<bool> SendAsync(SendFeedbackRequest request, string userAgent, CancellationToken token)
        {
            Request = request;
            UserAgent = userAgent;
            return Task.FromResult(sendResult);
        }
    }

    private sealed class RecordingAiHelperClient : IAiHelperClient
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
            return Task.FromResult((AiHelperStatus.Success, string.Empty));
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
