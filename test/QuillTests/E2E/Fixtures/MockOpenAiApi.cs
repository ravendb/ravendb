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

public sealed class MockOpenAiApi : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string BaseAddress { get; }

    public (string Name, string Arguments)? ToolCall { get; set; }

    private MockOpenAiApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public void Reset()
    {
        ToolCall = null;
    }

    public static async Task<MockOpenAiApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        MockOpenAiApi instance = null!;

        foreach (var route in new[] { "/chat/completions", "/v1/chat/completions" })
        {
            app.MapPost(route, async (HttpContext ctx) =>
            {
                using var reader = new StreamReader(ctx.Request.Body, leaveOpen: true);
                var body = await reader.ReadToEndAsync(ctx.RequestAborted);

                ctx.Response.ContentType = "text/event-stream";
                await WriteEventAsync(ctx, instance.BuildChunk(body));
                await WriteEventAsync(ctx, UsageChunk);
                await WriteRawAsync(ctx, "data: [DONE]\n\n");
            });
        }

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockOpenAiApi failed to bind a port.");

        instance = new MockOpenAiApi(app, url.TrimEnd('/'));
        return instance;
    }

    private string BuildChunk(string requestBody)
    {
        var toolResult = TryGetLastToolContent(requestBody);
        if (toolResult is not null)
            return ContentChunk($"{{\"reply\":{JsonSerializer.Serialize(toolResult)}}}");

        if (ToolCall is not { } call)
            return ContentChunk("{\"reply\":\"nothing to do\"}");

        var function = new JsonObject { ["name"] = call.Name, ["arguments"] = call.Arguments };
        var toolCall = new JsonObject
        {
            ["index"] = 0,
            ["id"] = "call_mock",
            ["type"] = "function",
            ["function"] = function,
        };
        return Chunk(new JsonObject { ["tool_calls"] = new JsonArray(toolCall) });
    }

    private static string? TryGetLastToolContent(string requestBody)
    {
        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(requestBody);
        }
        catch (JsonException)
        {
            return null;
        }

        if (parsed?["messages"] is not JsonArray messages)
            return null;

        string? content = null;
        foreach (var message in messages)
        {
            if ((string?)message?["role"] == "tool")
                content = (string?)message?["content"];
        }

        return content;
    }

    private static string ContentChunk(string content) => Chunk(new JsonObject { ["content"] = content });

    private static string Chunk(JsonObject delta)
    {
        var payload = new JsonObject
        {
            ["id"] = "chatcmpl-mock",
            ["object"] = "chat.completion.chunk",
            ["model"] = "gpt-4o-mock",
            ["choices"] = new JsonArray(new JsonObject { ["index"] = 0, ["delta"] = delta }),
        };
        return payload.ToJsonString();
    }

    private const string UsageChunk = """
        {"id":"chatcmpl-mock","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}
        """;

    private static Task WriteEventAsync(HttpContext ctx, string json) => WriteRawAsync(ctx, $"data: {json}\n\n");

    private static async Task WriteRawAsync(HttpContext ctx, string text)
    {
        await ctx.Response.Body.WriteAsync(Encoding.UTF8.GetBytes(text), ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
