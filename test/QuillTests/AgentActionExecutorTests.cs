using FastTests;
using Microsoft.Extensions.Logging.Abstractions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Quill.Logging;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ActionFixtures;

namespace QuillTests;

// no appliance needed: the executor is constructed directly and talks only to a mock HTTP receiver
public class AgentActionExecutorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_delivered_body_is_the_model_arguments_and_nothing_else()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (200, """{"ticketId":"T-1"}""");

        var binding = Webhook(receiver.WebhookUrl, secret: "s3cret");
        var result = await WebhookExecutor().ExecuteAsync(ActionRequest(), binding, CancellationToken.None);

        // the status line carries any notable headers the receiver sent, so assert the two parts apart
        Assert.StartsWith("action succeeded: webhook returned 200", result);
        Assert.Equal("""{"ticketId":"T-1"}""", BodyOf(result));

        var delivery = Assert.Single(receiver.Deliveries);
        Assert.Equal("s3cret", delivery.Headers["X-Quill-Secret"]);

        // no envelope: the arguments are the whole body, verbatim
        Assert.Equal("""{"subject":"Broken"}""", delivery.Body.GetRawText());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_without_a_secret_omits_the_header()
    {
        await using var receiver = await MockQuillServices.StartAsync();

        await WebhookExecutor().ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.DoesNotContain("X-Quill-Secret", Assert.Single(receiver.Deliveries).Headers.Keys);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_non_2xx_is_reported_to_the_model()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (500, """{"error":"boom"}""");

        var result = await WebhookExecutor().ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.StartsWith("action failed: webhook returned 500", result);
        Assert.Equal("""{"error":"boom"}""", BodyOf(result));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Notable_response_headers_reach_the_model_and_the_rest_do_not()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (200, """{"ticketId":"T-1"}""");
        receiver.WebhookResponseHeaders["Location"] = "https://tickets.example/T-1";
        receiver.WebhookResponseHeaders["X-Powered-By"] = "not worth the model's tokens";

        var result = await WebhookExecutor().ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.Contains(Environment.NewLine + "Location: https://tickets.example/T-1", result);
        Assert.DoesNotContain("X-Powered-By", result);
        Assert.Equal("""{"ticketId":"T-1"}""", BodyOf(result));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_timeout_is_reported_to_the_model()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookDelay = TimeSpan.FromSeconds(30);

        var result = await WebhookExecutor(TimeSpan.FromSeconds(2))
            .ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.Equal("action failed: webhook timed out", result);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_connection_failure_is_reported_to_the_model()
    {
        // port 1 refuses connections
        var result = await WebhookExecutor().ExecuteAsync(
            ActionRequest(), Webhook("http://127.0.0.1:1/hook"), CancellationToken.None);

        // the transport reason reaches the model verbatim, so match only the shared prefix
        Assert.StartsWith("action failed:", result);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_oversized_webhook_response_is_capped_and_says_so()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (200, new string('x', 100 * 1024));

        var result = await WebhookExecutor().ExecuteAsync(
            ActionRequest(), Webhook(receiver.WebhookUrl, maxResponseSize: 64), CancellationToken.None);

        Assert.EndsWith(TruncationMarker, result);
        Assert.Equal(new string('x', 64) + TruncationMarker, BodyOf(result));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_binding_without_a_cap_falls_back_to_4KB()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (200, new string('x', 100 * 1024));

        var result = await WebhookExecutor().ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.Equal(4 * 1024 + TruncationMarker.Length, BodyOf(result).Length);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_cap_landing_mid_codepoint_does_not_corrupt_the_response()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        // '€' is 3 bytes, so a 64-byte cut lands inside a character rather than between two
        receiver.WebhookResponse = (200, new string('€', 1024));

        var result = await WebhookExecutor().ExecuteAsync(
            ActionRequest(), Webhook(receiver.WebhookUrl, maxResponseSize: 64), CancellationToken.None);

        Assert.DoesNotContain('�', result);
        Assert.EndsWith(TruncationMarker, result);
        Assert.Equal(new string('€', 64 / 3) + TruncationMarker, BodyOf(result));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_webhook_that_succeeds_with_no_content_still_answers_the_model()
    {
        await using var receiver = await MockQuillServices.StartAsync();
        receiver.WebhookResponse = (204, "");

        // the client rejects an empty action response, so "" must never escape the executor
        var result = await WebhookExecutor().ExecuteAsync(ActionRequest(), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.StartsWith("action succeeded: webhook returned 204 with no content", result);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Missing_arguments_are_delivered_as_an_empty_object()
    {
        await using var receiver = await MockQuillServices.StartAsync();

        await WebhookExecutor().ExecuteAsync(
            ActionRequest(arguments: null), Webhook(receiver.WebhookUrl), CancellationToken.None);

        Assert.Equal("{}", Assert.Single(receiver.Deliveries).Body.GetRawText());
    }

    // mirrors the literals the executor appends; it has no constants to reference.
    // the executor separates with Environment.NewLine, so a "\n" literal here would only pass on Linux
    private static readonly string TruncationMarker = Environment.NewLine + "[truncated]";

    // the model is handed a status line plus any notable headers, then the body after a blank line
    private static readonly string BodySeparator = Environment.NewLine + Environment.NewLine;

    private static string BodyOf(string result) =>
        result[(result.IndexOf(BodySeparator, StringComparison.Ordinal) + BodySeparator.Length)..];

    // QuillLogger reads the process-wide RavenLogManager, which is unconfigured here, so it is a no-op
    private WebhookActionExecutor WebhookExecutor(TimeSpan? timeout = null) =>
        new(new SingleClientFactory(new HttpClient { Timeout = timeout ?? TimeSpan.FromSeconds(30) }),
            new QuillLogger<WebhookActionExecutor>());

    private sealed class SingleClientFactory(HttpClient client) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => client;
    }

    private static AiAgentActionRequest ActionRequest(string? arguments = """{"subject":"Broken"}""") =>
        new() { Name = "create_ticket", ToolId = "call_1", Arguments = arguments };
}
