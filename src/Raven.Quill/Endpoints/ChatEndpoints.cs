using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

/// Lifted from nopcommerce-demo/cdc-bridge/src/Web/ChatRoutes.cs. Now:
///   - resolves dependencies from DI instead of a captured factory lambda;
///   - resolves the named agent from the config database and runs it through
///     the shared IAgentRouter (data-driven, no compile-time schema);
///   - lives under /api/chat/* per the design doc §1.4 URL convention.
///
/// NDJSON over text/event-stream because EventSource is GET-only and we need
/// a JSON body. Each line is a self-contained `{type:chunk|done|error}`.
public static class ChatEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/chat").WithTags("chat").RequireAuthorization();
        group.MapPost("/stream", HandleStreamAsync)
            .WithName("chat.stream")
            .Accepts<ChatRequest>("application/json")
            // Streams NDJSON frames, not a single string — declare the status +
            // content type only, without a (misleading) string body schema.
            .Produces(StatusCodes.Status200OK, contentType: "application/x-ndjson")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
    }

    private static async Task HandleStreamAsync(
        HttpContext ctx,
        IDocumentStore store,
        IAgentRouter router,
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
        catch (Exception)
        {
            // Generic message only — a raw parser/exception message is internal detail we don't echo.
            await WriteBadRequestAsync(ctx, "invalid JSON body");
            return;
        }

        if (body is null || string.IsNullOrWhiteSpace(body.AgentId) || string.IsNullOrWhiteSpace(body.Prompt))
        {
            await WriteBadRequestAsync(ctx, "agentId and prompt are required");
            return;
        }

        // The legacy chat surface stays bound to the config database (per-app
        // agents are reached via embed / setup-try). Resolve the agent up front
        // so an unknown id is a clean 400 before the NDJSON stream opens.
        var config = await AgentLookup.FindAsync(store, store.Database, body.AgentId, ctx.RequestAborted);
        if (config is null)
        {
            await WriteBadRequestAsync(ctx, $"unknown agentId '{body.AgentId}'");
            return;
        }

        // Validate + normalize the conversation id (the single rule lives on
        // AgentRouter): empty/missing -> "chats/" lets RavenDB auto-allocate;
        // otherwise the value must begin with "chats/" so a caller can't pass
        // e.g. `users/admin` and overwrite an unrelated document.
        if (AgentRouter.TryNormalizeConversationId(body.ConversationId, out var conversationId, out var conversationError) == false)
        {
            await WriteBadRequestAsync(ctx, conversationError!);
            return;
        }

        NdjsonStream.SetHeaders(ctx);

        try
        {
            var result = await router.RunAsync(
                new AgentRequest(store.Database, config.Identifier, conversationId, body.Prompt, body.Parameters),
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ctx.RequestAborted,
                resolved: config);

            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                answer = result.Answer,
                conversationId = result.ConversationId,
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
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Chat stream failed. See server logs for details." });
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
}
