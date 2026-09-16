using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Contracts;
using Raven.Quill.Feedback;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillFeedbackCollection.Name)]
public class SettingsEndpointsTests(ITestOutputHelper output, QuillFeedbackFixture fixture)
    : QuillFeedbackTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task License_surfaces_server_license_connectivity_and_plans()
    {
        var license = await Host.GetLicenseAsync();

        // response: the server's /license/status, projected onto ServerLicenseResponse.
        Assert.False(string.IsNullOrEmpty(license.Response.Status));  // e.g. "Commercial" / "AGPL - Open Source"
        Assert.False(string.IsNullOrEmpty(license.Response.Type));    // e.g. "EnterpriseAi" / "None"

        // connectivity: the server's /license-server/connectivity probe.
        Assert.False(string.IsNullOrEmpty(license.Connectivity.StatusCode));

        // plans: the static catalog LicenseStatsProvider always appends.
        Assert.True(license.Plans.Length >= 1);
        Assert.Equal("enterprise", license.Plans[0].Slug);
        Assert.True(license.Plans[0].Features.Length >= 1);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_returns_quill_usage_payload()
    {
        // ?year=&month=[&day=] — forwarded to RavenDB's /admin/license/quill/usage as year+month.
        // A well-formed QuillUsageResponse { PerApplication, ByPeriod } shape must deserialize (values may be
        // null when the server reports no usage); the typed read throws on a non-2xx or malformed body.
        var usage = await Host.GetSettingsUsageAsync(2026, month: 5);
        Assert.NotNull(usage);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_supports_the_year_view()
    {
        // year only → the whole-year view; like the month view it proxies straight to /admin/license/quill/usage.
        var usage = await Host.GetSettingsUsageAsync(2026);
        Assert.NotNull(usage);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_accepts_only_user_fields_and_normalizes_them()
    {
        using var client = Host.Factory.CreateClient();
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
        Assert.NotNull(Feedback.Request);
        Assert.Equal("Jane Doe", Feedback.Request.Name);
        Assert.Equal("user@example.com", Feedback.Request.Email);
        Assert.Equal("positive", Feedback.Request.Impression);
        Assert.Equal("Please contact me.", Feedback.Request.Message);
        Assert.Equal("/dashboard/license", Feedback.Request.StudioView);
        Assert.Contains("Quill-Test/1.0", Feedback.UserAgent);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_treats_impression_and_studio_view_as_optional()
    {
        using var client = Host.Factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = "Jane Doe",
            Email = "user@example.com",
            Impression = " ",
            Message = "Just a question.",
        });

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(Feedback.Request);
        Assert.Null(Feedback.Request.Impression);
        Assert.Null(Feedback.Request.StudioView);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(" ", "user@example.com", "positive", "Message.")]
    [InlineData("Jane Doe", "not-an-email", "positive", "Message.")]
    [InlineData("Jane Doe", "user@example.com", "meh", "Message.")]
    [InlineData("Jane Doe", "user@example.com", "positive", " ")]
    public async Task Feedback_rejects_invalid_input(string name, string email, string impression, string message)
    {
        using var client = Host.Factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = name,
            Email = email,
            Impression = impression,
            Message = message,
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(Feedback.Request);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(257, 254, 8_192, 512)]   // name over its cap
    [InlineData(256, 255, 8_192, 512)]   // email over its cap
    [InlineData(256, 254, 8_193, 512)]   // message over its cap
    [InlineData(256, 254, 8_192, 513)]   // studio view over its cap
    public async Task Feedback_rejects_over_long_fields(int nameLength, int emailLength, int messageLength, int studioViewLength)
    {
        using var client = Host.Factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = new string('n', nameLength),
            Email = "u@" + new string('d', emailLength - 6) + ".com",
            Impression = "positive",
            Message = new string('m', messageLength),
            StudioView = new string('v', studioViewLength),
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(Feedback.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_accepts_fields_at_their_length_caps()
    {
        using var client = Host.Factory.CreateClient();

        HttpResponseMessage response = await client.PostAsJsonAsync("/api/settings/feedback", new
        {
            Name = new string('n', 256),
            Email = "u@" + new string('d', 254 - 6) + ".com",
            Impression = "positive",
            Message = new string('m', 8_192),
            StudioView = new string('v', 512),
        });

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(Feedback.Request);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Feedback_returns_bad_gateway_when_sending_fails()
    {
        using var client = Host.Factory.CreateClient();
        Feedback.SendResult = false;

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
}
