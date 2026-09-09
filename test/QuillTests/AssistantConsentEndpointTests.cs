using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillAssistantCollection.Name)]
public class AssistantConsentEndpointTests(ITestOutputHelper output, QuillAiHelperFixture fixture)
    : QuillAiHelperTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Check_reports_the_consent_the_license_has_already_given()
    {
        var response = await Host.Client.GetAsync(AssistantConsentRoute);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Success", await ReadStatusAsync(response));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Check_answers_a_missing_consent_with_200_so_the_browser_keeps_its_session()
    {
        Mock.RequireConsentForAssist = true;

        var response = await Host.Client.GetAsync(AssistantConsentRoute);

        // The AI service refuses with a 401, but relaying that would look like an expired Quill
        // session to the SPA, which signs the operator out on any 401.
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("ConsentRequired", await ReadStatusAsync(response));
        Assert.Equal(0, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Giving_consent_opens_the_gate_for_the_chat()
    {
        Mock.RequireConsentForAssist = true;
        Mock.ChatbotChunks = ["Answered after consent."];

        var granted = await Host.Client.PostAsJsonAsync(AssistantConsentRoute, new { });

        Assert.Equal(HttpStatusCode.OK, granted.StatusCode);
        Assert.Equal("Success", await ReadStatusAsync(granted));
        Assert.Equal(1, Mock.GiveConsentCallCount);

        var rechecked = await Host.Client.GetAsync(AssistantConsentRoute);
        Assert.Equal("Success", await ReadStatusAsync(rechecked));

        var chat = await Host.Client.PostAsJsonAsync("/api/assistant/chat", new { message = "hi" });
        Assert.Equal(HttpStatusCode.OK, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Giving_consent_reports_a_license_the_service_rejects()
    {
        Mock.RequireConsentForAssist = true;
        Mock.GiveConsentResponse = (401, """{"Status":"InvalidCredentials"}""");

        var response = await Host.Client.PostAsJsonAsync(AssistantConsentRoute, new { });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("InvalidCredentials", await ReadStatusAsync(response));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Check_answers_502_when_the_service_says_nothing_it_can_read()
    {
        Mock.CheckConsentResponse = (200, "not json");

        var response = await Host.Client.GetAsync(AssistantConsentRoute);

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("The AI service could not be reached.", body.RootElement.GetProperty("error").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Check_answers_502_when_the_service_is_unreachable()
    {
        // own host: points at a dead address instead of the shared mock
        await using var host = await NewHostAsync(
            configure: opts => opts.AiApiUrl = "http://nonexistent.invalid",
            setupPackagePath: NewDataPath(forceCreateDir: true));

        var response = await host.Client.GetAsync(AssistantConsentRoute);

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
    }

    private const string AssistantConsentRoute = "/api/assistant/consent";

    private static async Task<string?> ReadStatusAsync(HttpResponseMessage response)
    {
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return body.RootElement.GetProperty("status").GetString();
    }
}
