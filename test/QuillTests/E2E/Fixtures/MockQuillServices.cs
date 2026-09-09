using System.Collections.Concurrent;
using System.Text;
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

public abstract record MockLlmTurn;

/// The model asks for an action. <paramref name="ArgumentsJson"/> is what it "sends" as the tool arguments.
public sealed record ToolCallTurn(string ActionName, string ArgumentsJson, string? ToolId = null) : MockLlmTurn;

/// The model's final answer. <paramref name="ContentJson"/> must match the agent's output schema,
/// e.g. <c>{"reply":"done"}</c>.
public sealed record FinalTurn(string ContentJson) : MockLlmTurn;

/// <summary>
/// Every third party in the Quill topology, on one in-process host bound to an ephemeral loopback port
/// ⇒ parallel-safe. Each is reached over a real socket by a different caller, which is why they cannot be
/// in-memory: <c>/assistant/*</c> by Quill's outbound client, <c>/chat/completions</c> by RavenDB, and
/// <c>/hook</c> by the action executor. A test configures only the parts it uses. Caller disposes.
/// </summary>
public sealed class MockQuillServices : IAsyncDisposable
{
    public sealed record Delivery(Dictionary<string, string> Headers, JsonElement Body);

    private WebApplication? _app;
    private readonly ConcurrentQueue<MockLlmTurn> _turns = new();
    private readonly List<JsonElement> _completionRequests = [];
    private readonly List<Delivery> _deliveries = [];
    private readonly Lock _sync = new();
    private bool _consentGiven;

    public string BaseAddress { get; private set; } = "";

    // ---- /assistant/* — the bundled RavenDB's AI proxy hop, as Quill sees it ----

    public string? LastCdcRequestBody { get; private set; }

    public string? LastAgentRequestBody { get; private set; }

    public string? LastChatbotRequestBody { get; private set; }

    public (int Status, string Body) CdcResponse { get; set; } = (200, "{}");

    public (int Status, string Body) AgentResponse { get; set; } = (200, "{}");

    /// The Ongoing frames a Chatbot assist streams; the answer is their concatenation.
    public string[] ChatbotChunks { get; set; } = [];

    /// The Done frame payload, i.e. the chatbot result; null ends the stream without one.
    public string? ChatbotResult { get; set; } = ChatbotResultBody();

    /// When set, a Chatbot assist fails with this status and body instead of streaming. The content type
    /// is the caller's to state: the service answers some refusals in JSON and others in plain text.
    public (int Status, string Body, string ContentType)? ChatbotFailure { get; set; }

    /// When true, a Chatbot assist drops the connection, so Quill sees a transport failure.
    public bool ChatbotAbortsConnection { get; set; }

    /// When set, this frame is streamed after the chunks in place of the Done frame.
    public string? ChatbotErrorFrame { get; set; }

    // one line: the reader on the other end parses SSE line by line
    public static string ChatbotResultBody(
        string conversationId = "conversations/1",
        string status = "Success",
        string relevantLinks = "[]",
        string followUpQuestions = "[]",
        double usagePercentage = 1.5) =>
        $$$"""{"ConversationId":"{{{conversationId}}}","Status":"{{{status}}}","UsagePercentage":{{{usagePercentage}}},"Response":{"Answer":"","RelevantLinks":{{{relevantLinks}}},"FollowUpQuestions":{{{followUpQuestions}}}}}""";

    /// Simulates slow LLM generation.
    public TimeSpan AssistDelay { get; set; }

    /// When true, assist and check-consent answer 401 ConsentRequired until give-consent is called.
    public bool RequireConsentForAssist { get; set; }

    public (int Status, string Body) GiveConsentResponse { get; set; } = (200, "{\"Status\":\"Success\"}");

    /// When set, check-consent answers this instead of reporting whether the gate is open.
    public (int Status, string Body)? CheckConsentResponse { get; set; }

    public int GiveConsentCallCount { get; private set; }

    // ---- /hook — a customer's action webhook ----

    public string WebhookUrl => BaseAddress + "/hook";

    public (int Status, string Body) WebhookResponse { get; set; } = (200, """{"ok":true}""");

    /// Extra response headers the receiver sets, e.g. Location or Retry-After.
    public Dictionary<string, string> WebhookResponseHeaders { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// Simulates a slow receiver.
    public TimeSpan WebhookDelay { get; set; }

    public IReadOnlyList<Delivery> Deliveries
    {
        get
        {
            lock (_sync)
                return _deliveries.ToArray();
        }
    }

    private int DeliveryCount
    {
        get
        {
            lock (_sync)
                return _deliveries.Count;
        }
    }

    /// Polls until at least <paramref name="count"/> deliveries have landed; throws on timeout so a
    /// stalled receiver fails the test instead of hanging it.
    public async Task WaitForDeliveriesAsync(int count, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DeliveryCount < count)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException(
                    $"Expected {count} webhook delivery(ies) within {timeout.TotalSeconds:0.#}s, saw {DeliveryCount}.");

            await Task.Delay(25);
        }
    }

    // ---- /chat/completions — the LLM RavenDB talks to ----

    /// The tool result the model was fed last — i.e. what the action executor produced.
    public string? LastToolMessageContent()
    {
        List<JsonElement> requests;
        lock (_sync)
            requests = [.. _completionRequests];

        for (var i = requests.Count - 1; i >= 0; i--)
        {
            if (requests[i].TryGetProperty("messages", out var messages) == false)
                continue;

            string? content = null;
            foreach (var message in messages.EnumerateArray())
            {
                if (message.TryGetProperty("role", out var role) && role.GetString() == "tool")
                    content = message.TryGetProperty("content", out var c) ? c.GetString() : null;
            }

            if (content is not null)
                return content;
        }

        return null;
    }

    /// <param name="turns">LLM replies, handed out in order, one per completion request.</param>
    public static async Task<MockQuillServices> StartAsync(params MockLlmTurn[] turns)
    {
        var mock = new MockQuillServices();
        foreach (var turn in turns)
            mock._turns.Enqueue(turn);

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        mock._app = builder.Build();
        mock.MapRoutes(mock._app);

        await mock._app.StartAsync();

        var addresses = mock._app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        mock.BaseAddress = addresses?.Addresses.FirstOrDefault()?.TrimEnd('/')
                           ?? throw new InvalidOperationException($"{nameof(MockQuillServices)} failed to bind a port.");

        return mock;
    }

    /// Only the collection-shared instances need this; a per-test instance is disposed instead.
    public void Reset()
    {
        LastCdcRequestBody = null;
        LastAgentRequestBody = null;
        LastChatbotRequestBody = null;
        CdcResponse = (200, "{}");
        AgentResponse = (200, "{}");
        ChatbotChunks = [];
        ChatbotResult = ChatbotResultBody();
        ChatbotFailure = null;
        ChatbotAbortsConnection = false;
        ChatbotErrorFrame = null;
        AssistDelay = TimeSpan.Zero;
        RequireConsentForAssist = false;
        GiveConsentResponse = (200, "{\"Status\":\"Success\"}");
        CheckConsentResponse = null;
        GiveConsentCallCount = 0;
        _consentGiven = false;

        WebhookResponse = (200, """{"ok":true}""");
        WebhookResponseHeaders.Clear();
        WebhookDelay = TimeSpan.Zero;
        _turns.Clear();
        lock (_sync)
        {
            _deliveries.Clear();
            _completionRequests.Clear();
        }
    }

    private void MapRoutes(WebApplication app)
    {
        // inline async block lambda (not a method/local function) so minimal APIs bind it as an
        // IResult-returning route handler that actually writes the response
        app.MapPost("/assistant/assist", async (HttpContext ctx) =>
        {
            var body = await ReadBodyAsync(ctx);

            string? operationType;
            try
            {
                operationType = (string?)JsonNode.Parse(body)?["OperationType"];
            }
            catch (JsonException)
            {
                return Results.BadRequest("Malformed JSON body.");
            }

            if (AssistDelay > TimeSpan.Zero)
                await Task.Delay(AssistDelay, ctx.RequestAborted);

            switch (operationType)
            {
                case "CdcConfigSetup":
                    LastCdcRequestBody = body;
                    if (RequireConsentForAssist && _consentGiven == false)
                        return Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401);
                    return Results.Content(CdcResponse.Body, "application/json", statusCode: CdcResponse.Status);
                case "CdcBasedAgentConfigSetup":
                    LastAgentRequestBody = body;
                    if (RequireConsentForAssist && _consentGiven == false)
                        return Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401);
                    return Results.Content(AgentResponse.Body, "application/json", statusCode: AgentResponse.Status);
                case "Chatbot":
                    LastChatbotRequestBody = body;
                    if (RequireConsentForAssist && _consentGiven == false)
                        return Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401);
                    if (ChatbotAbortsConnection)
                    {
                        ctx.Abort();
                        return Results.Empty;
                    }
                    if (ChatbotFailure is { } failure)
                        return Results.Content(failure.Body, failure.ContentType, statusCode: failure.Status);
                    await WriteChatbotStreamAsync(ctx);
                    return Results.Empty;
                default:
                    return Results.BadRequest($"Unknown OperationType '{operationType}'.");
            }
        });

        // the appliance asks here before it lets an operator chat; 401 until the gate is open
        app.MapGet("/assistant/check-consent", () =>
        {
            if (CheckConsentResponse is { } configured)
                return Results.Content(configured.Body, "application/json", statusCode: configured.Status);

            return RequireConsentForAssist && _consentGiven == false
                ? Results.Content("{\"Status\":\"ConsentRequired\"}", "application/json", statusCode: 401)
                : Results.Content("{\"Status\":\"Success\"}", "application/json");
        });

        // the appliance posts here once an operator accepted the terms; a 200 opens the gate
        app.MapPost("/assistant/give-consent", () =>
        {
            GiveConsentCallCount++;
            var (status, body) = GiveConsentResponse;
            if (status is >= 200 and < 300)
                _consentGiven = true;
            return Results.Content(body, "application/json", statusCode: status);
        });

        app.MapPost("/hook", async (HttpContext ctx) =>
        {
            var body = await ReadBodyAsync(ctx);
            var headers = ctx.Request.Headers.ToDictionary(h => h.Key, h => h.Value.ToString(), StringComparer.OrdinalIgnoreCase);

            using var parsed = JsonDocument.Parse(body);
            lock (_sync)
                _deliveries.Add(new Delivery(headers, parsed.RootElement.Clone()));

            if (WebhookDelay > TimeSpan.Zero)
                await Task.Delay(WebhookDelay, ctx.RequestAborted);

            foreach (var (name, value) in WebhookResponseHeaders)
                ctx.Response.Headers[name] = value;

            var (status, responseBody) = WebhookResponse;
            return Results.Content(responseBody, "application/json", statusCode: status);
        });

        // a custom OpenAI endpoint resolves to {base}/chat/completions; /v1 is accepted too so the
        // fixture works regardless of how the connection string spells the base address
        app.MapPost("/chat/completions", CompleteAsync);
        app.MapPost("/v1/chat/completions", CompleteAsync);
    }

    /// The chatbot answer as the real service delivers it: SSE Ongoing frames, then one Done frame.
    private async Task WriteChatbotStreamAsync(HttpContext ctx)
    {
        ctx.Response.ContentType = "text/event-stream";

        foreach (var chunk in ChatbotChunks)
            await WriteSseFrameAsync(ctx, $"{{\"type\":\"Ongoing\",\"text\":{JsonSerializer.Serialize(chunk)}}}");

        if (ChatbotErrorFrame is not null)
            await WriteSseFrameAsync(ctx, ChatbotErrorFrame);
        else if (ChatbotResult is not null)
            await WriteSseFrameAsync(ctx, $"{{\"type\":\"Done\",\"text\":{ChatbotResult}}}");
    }

    private static async Task WriteSseFrameAsync(HttpContext ctx, string json)
    {
        await ctx.Response.WriteAsync($"data: {json}\n\n", ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    private async Task CompleteAsync(HttpContext ctx)
    {
        using var request = JsonDocument.Parse(await ReadBodyAsync(ctx));
        lock (_sync)
            _completionRequests.Add(request.RootElement.Clone());

        if (_turns.TryDequeue(out var turn) == false)
        {
            // an unscripted call means the test's expectations drifted — say so instead of hanging
            await WriteErrorAsync(ctx, "no scripted turn left for this request");
            return;
        }

        // Quill only ever reaches an LLM through StreamAsync, so a non-streaming request means the
        // product changed shape — say so rather than inventing a completion the tests never assert on
        if (request.RootElement.TryGetProperty("stream", out var stream) == false || stream.GetBoolean() == false)
        {
            await WriteErrorAsync(ctx, "only streaming requests are supported");
            return;
        }

        ctx.Response.ContentType = "text/event-stream";

        foreach (var chunk in BuildStreamedChunks(turn))
            await WriteEventAsync(ctx, chunk);

        await ctx.Response.WriteAsync("data: [DONE]\n\n", ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    private static Task WriteErrorAsync(HttpContext ctx, string message)
    {
        ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
        return ctx.Response.WriteAsJsonAsync(new
        {
            error = new { message = $"{nameof(MockQuillServices)}: {message}" }
        });
    }

    private static async Task WriteEventAsync(HttpContext ctx, object chunk)
    {
        var payload = JsonSerializer.Serialize(chunk);
        await ctx.Response.WriteAsync($"data: {payload}\n\n", Encoding.UTF8, ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    private static IEnumerable<object> BuildStreamedChunks(MockLlmTurn turn)
    {
        switch (turn)
        {
            case ToolCallTurn toolCall:
                yield return Chunk(new
                {
                    role = "assistant",
                    tool_calls = new[]
                    {
                        new
                        {
                            index = 0,
                            id = toolCall.ToolId ?? "call_" + Guid.NewGuid().ToString("N")[..8],
                            type = "function",
                            function = new { name = toolCall.ActionName, arguments = toolCall.ArgumentsJson },
                        }
                    },
                });
                yield return Chunk(new { }, finishReason: "tool_calls");
                break;

            case FinalTurn final:
                yield return Chunk(new { role = "assistant", content = final.ContentJson });
                yield return Chunk(new { }, finishReason: "stop");
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(turn), turn, "unknown scripted turn");
        }
    }

    private static object Chunk(object delta, string? finishReason = null) => new
    {
        id = "cmpl-mock",
        @object = "chat.completion.chunk",
        created = 1,
        model = "mock-model",
        choices = new[] { new { index = 0, delta, finish_reason = finishReason } },
        usage = new { prompt_tokens = 1, completion_tokens = 1, total_tokens = 2 },
    };

    /// leaveOpen: true so disposing the reader does not dispose the pipeline-owned body stream.
    private static async Task<string> ReadBodyAsync(HttpContext ctx)
    {
        using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
        return await reader.ReadToEndAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is null)
            return;

        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
