using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;

namespace Raven.AiAppliance.Endpoints;

/// Lifted from nopcommerce-demo/cdc-bridge/src/Web/ChatRoutes.cs. Now:
///   - resolves dependencies from DI instead of a captured factory lambda;
///   - dispatches to whichever IAgentSchema the request names;
///   - lives under /api/chat/* per the design doc §1.4 URL convention.
///
/// NDJSON over text/event-stream because EventSource is GET-only and we need
/// a JSON body. Each line is a self-contained `{type:chunk|done|error}`.
public static class ChatEndpoints
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // Default JavaScriptEncoder escapes HTML-sensitive characters: a
        // literal '<' is emitted in the JSON output as the six-character
        // escape sequence backslash-u-0-0-3-C, which JSON.parse decodes back
        // to '<' transparently on the client. Safe to fall back to default —
        // no information is lost on the wire; avoids XSS exposure if any
        // downstream consumer ever embeds chat output into an HTML context.
        // Demo answer types use public fields, not properties, so the RavenDB
        // schema generator can read the initializers. System.Text.Json needs
        // opt-in to serialize them.
        IncludeFields = true,
    };

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/chat").WithTags("chat");
        group.MapPost("/stream", HandleStreamAsync)
            .WithName("chat.stream")
            .Accepts<ChatRequest>("application/json")
            .Produces<string>(StatusCodes.Status200OK, contentType: "application/x-ndjson")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
    }

    private static async Task HandleStreamAsync(
        HttpContext ctx,
        IDocumentStore store,
        IAgentSchemaRegistry schemas,
        ILogger<ChatStreamLogger> logger)
    {
        ChatRequest? body;
        try
        {
            body = await ctx.Request.ReadFromJsonAsync<ChatRequest>(ctx.RequestAborted);
        }
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
        {
            // Client disconnected before the body was fully read. Don't try to
            // write a 400 — the response is already aborted, and WriteAsJsonAsync
            // would throw a second exception that flooded the logs in earlier
            // versions of this handler.
            return;
        }
        catch (Exception e)
        {
            await WriteBadRequestAsync(ctx, $"invalid JSON body: {e.Message}");
            return;
        }

        if (body is null || string.IsNullOrWhiteSpace(body.AgentId) || string.IsNullOrWhiteSpace(body.Prompt))
        {
            await WriteBadRequestAsync(ctx, "agentId and prompt are required");
            return;
        }

        if (!schemas.TryGet(body.AgentId, out var schema))
        {
            await WriteBadRequestAsync(ctx, $"unknown agentId '{body.AgentId}'");
            return;
        }

        // Pin client-supplied conversation IDs to the "chats/" prefix.
        // Without this, a caller could pass `conversationId: "users/admin"`
        // and overwrite an unrelated document. Empty/missing → "chats/" lets
        // RavenDB auto-allocate. Otherwise the value must begin with "chats/".
        var conversationId = string.IsNullOrWhiteSpace(body.ConversationId)
            ? "chats/"
            : body.ConversationId;
        if (!conversationId.StartsWith("chats/", StringComparison.Ordinal))
        {
            await WriteBadRequestAsync(ctx, "conversationId must start with 'chats/'");
            return;
        }

        ctx.Response.ContentType = "application/x-ndjson; charset=utf-8";
        ctx.Response.Headers["Cache-Control"] = "no-cache";
        ctx.Response.Headers["X-Accel-Buffering"] = "no";

        try
        {
            var creationOptions = new AiConversationCreationOptions();
            if (body.Parameters is not null)
            {
                foreach (var (key, value) in body.Parameters)
                    creationOptions.AddParameter(key, value);
            }

            var conversation = store.AI.Conversation(
                agentId:         schema.Identifier,
                conversationId:  conversationId,
                creationOptions: creationOptions);

            conversation.AddUserPrompt(body.Prompt);

            var answer = await schema.RunConversationAsync(
                conversation,
                async chunk => await WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ctx.RequestAborted);

            await WriteLineAsync(ctx, new
            {
                type           = "done",
                answer,
                conversationId = conversation.Id,
            });
        }
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
        {
            // Client disconnected mid-stream. Nothing useful to write.
        }
        catch (Exception e)
        {
            // Log full exception server-side; surface only a generic
            // message to the client. Raw e.Message from RavenDB / agent /
            // downstream services can include file paths, connection
            // strings, DB names, and other internal detail we shouldn't
            // disclose over the chat stream.
            logger.LogError(e, "Chat stream failed for agentId={AgentId}", body.AgentId);
            try
            {
                await WriteLineAsync(ctx, new { type = "error", message = "Chat stream failed. See server logs for details." });
            }
            catch
            {
                // Response may already be partially flushed.
            }
        }
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class ChatStreamLogger;

    private static async Task WriteBadRequestAsync(HttpContext ctx, string error)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse(error));
    }

    private static async Task WriteLineAsync(HttpContext ctx, object payload)
    {
        var json = JsonSerializer.Serialize(payload, JsonOpts);
        var bytes = Encoding.UTF8.GetBytes(json + "\n");
        await ctx.Response.Body.WriteAsync(bytes, ctx.RequestAborted);
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }
}
