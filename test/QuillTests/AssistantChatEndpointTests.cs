using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillAssistantCollection.Name)]
public class AssistantChatEndpointTests(ITestOutputHelper output, QuillAiHelperFixture fixture)
    : QuillAiHelperTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_the_whole_assist_stream_untouched()
    {
        Mock.ChatbotChunks = ["RavenDB ", "is a database."];
        Mock.ChatbotResult = MockQuillServices.ChatbotResultBody(
            conversationId: "conversations/7",
            relevantLinks: """[{"Title":"Indexes","Url":"https://ravendb.net/docs/indexes"}]""",
            followUpQuestions: """["How do I create one?"]""",
            usagePercentage: 12.5);

        var (response, frames) = await ChatAsync(new { message = "What is RavenDB?", conversationId = (string?)null });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("text/event-stream", response.Content.Headers.ContentType?.ToString() ?? "");

        // The frames keep the AI service's own envelope and casing — Quill reshapes nothing.
        Assert.Equal(["Ongoing", "Ongoing", "Done"], frames.Select(frame => frame.GetProperty("type").GetString()));
        Assert.Equal("RavenDB ", frames[0].GetProperty("text").GetString());
        Assert.Equal("is a database.", frames[1].GetProperty("text").GetString());

        var result = frames[2].GetProperty("text");
        Assert.Equal("conversations/7", result.GetProperty("ConversationId").GetString());
        Assert.Equal("Success", result.GetProperty("Status").GetString());
        // The fields Quill used to drop on the floor now reach the client.
        Assert.Equal(12.5, result.GetProperty("UsagePercentage").GetDouble());
        Assert.Equal(
            "How do I create one?",
            result.GetProperty("Response").GetProperty("FollowUpQuestions").EnumerateArray().Single().GetString());
        var link = result.GetProperty("Response").GetProperty("RelevantLinks").EnumerateArray().Single();
        Assert.Equal("Indexes", link.GetProperty("Title").GetString());
        Assert.Equal("https://ravendb.net/docs/indexes", link.GetProperty("Url").GetString());

        var sent = JsonDocument.Parse(Mock.LastChatbotRequestBody!).RootElement;
        Assert.Equal("Chatbot", sent.GetProperty("OperationType").GetString());
        Assert.Equal("What is RavenDB?", sent.GetProperty("Message").GetString());
        Assert.True(sent.GetProperty("RavenVersion").GetInt32() > 0);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_forwards_the_conversation_id_of_a_follow_up_turn()
    {
        Mock.ChatbotChunks = ["Sure."];

        await ChatAsync(new { message = "And sharding?", conversationId = "conversations/7" });

        var sent = JsonDocument.Parse(Mock.LastChatbotRequestBody!).RootElement;
        Assert.Equal("conversations/7", sent.GetProperty("ConversationId").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_the_consent_refusal_instead_of_granting_consent()
    {
        Mock.RequireConsentForAssist = true;
        Mock.ChatbotChunks = ["Answered after consent."];

        var response = await Host.Client.PostAsJsonAsync(AssistantChatRoute, new { message = "hi" });

        // Consent is the operator's to give through /api/assistant/consent. The client reads the
        // upstream Status out of the body, the way the Studio does, and shows its consent gate.
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Equal("ConsentRequired", await ReadStatusAsync(response));
        Assert.Equal(0, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_an_exhausted_quota_with_its_status_code()
    {
        Mock.ChatbotFailure = (429, """{"Status":"OutOfTokens"}""", "application/json");

        var response = await Host.Client.PostAsJsonAsync(AssistantChatRoute, new { message = "hi" });

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.Equal("OutOfTokens", await ReadStatusAsync(response));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_a_stream_that_never_completed_as_it_arrived()
    {
        Mock.ChatbotChunks = ["Half an ans"];
        Mock.ChatbotResult = null;

        var (response, frames) = await ChatAsync(new { message = "hi", conversationId = (string?)null });

        // A truncated stream is not the proxy's to diagnose; the client sees no Done frame and says so.
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(["Ongoing"], frames.Select(frame => frame.GetProperty("type").GetString()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_returns_400_when_the_message_is_missing()
    {
        var response = await Host.Client.PostAsJsonAsync(AssistantChatRoute, new { message = "   " });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(Mock.LastChatbotRequestBody);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_a_message_the_service_finds_too_large()
    {
        // The service answers this one in plain text, so there is no Status for the client to read and
        // the content type has to survive the relay for it to fall back on the code instead.
        Mock.ChatbotFailure = (413, "Request body too large", "text/plain");

        // Quill caps nothing itself: how much the AI service will take is the service's to say.
        var response = await Host.Client.PostAsJsonAsync(
            AssistantChatRoute, new { message = new string('a', 64 * 1024) });

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal("text/plain", response.Content.Headers.ContentType?.MediaType);
        Assert.Equal("Request body too large", await response.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_relays_an_error_frame_as_it_arrived()
    {
        Mock.ChatbotChunks = ["Half an ans"];
        Mock.ChatbotErrorFrame = """{"type":"Error","text":"the model gave up"}""";

        var (response, frames) = await ChatAsync(new { message = "hi", conversationId = (string?)null });

        // Naming the failure is the client's job; the proxy just passes the frame along.
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(["Ongoing", "Error"], frames.Select(frame => frame.GetProperty("type").GetString()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_answers_502_in_Quills_own_error_shape_when_the_service_is_unreachable()
    {
        Mock.ChatbotAbortsConnection = true;

        var response = await Host.Client.PostAsJsonAsync(AssistantChatRoute, new { message = "hi" });

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        // Not the AI service's PascalCase Status: this body is Quill's, and it goes out through the app's
        // camelCase policy, so it has to be a shape the client reads that way.
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("The AI assistant is unavailable right now.", body.RootElement.GetProperty("error").GetString());
    }

    private const string AssistantChatRoute = "/api/assistant/chat";

    private const string DataPrefix = "data:";

    private async Task<(HttpResponseMessage Response, JsonElement[] Frames)> ChatAsync(object body)
    {
        // raw: the assistant chat relays Server-Sent Events — no typed happy wrapper to reuse
        var response = await Host.Client.PostAsJsonAsync(AssistantChatRoute, body);
        var sse = await response.Content.ReadAsStringAsync();

        var frames = sse
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => line.StartsWith(DataPrefix, StringComparison.Ordinal))
            .Select(line => JsonDocument.Parse(line[DataPrefix.Length..]).RootElement)
            .ToArray();

        return (response, frames);
    }

    private static async Task<string?> ReadStatusAsync(HttpResponseMessage response)
    {
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return body.RootElement.GetProperty("Status").GetString();
    }
}
