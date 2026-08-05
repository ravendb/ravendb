using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

public static class ChatEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/chat").WithTags("chat").RequireAuthorization();
        // NDJSON not SSE: EventSource is GET-only and we POST a JSON body
        group.MapPost("/stream", HandleStreamAsync)
            .WithName("chat.stream")
            .Accepts<ChatRequest>("application/json")
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
        // don't write on an aborted response: a second exception floods logs
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
        {
            return;
        }
        catch (Exception)
        {
            await WriteBadRequestAsync(ctx, "invalid JSON body");
            return;
        }

        if (body is null || string.IsNullOrWhiteSpace(body.AgentId) || string.IsNullOrWhiteSpace(body.Prompt))
        {
            await WriteBadRequestAsync(ctx, "agentId and prompt are required");
            return;
        }

        var config = await AgentLookup.FindAsync(store, store.Database, body.AgentId, ctx.RequestAborted);
        if (config is null)
        {
            await WriteBadRequestAsync(ctx, $"unknown agentId '{body.AgentId}'");
            return;
        }

        if (AgentRouter.TryNormalizeConversationId(body.ConversationId, out var conversationId, out var conversationError) == false)
        {
            await WriteBadRequestAsync(ctx, conversationError!);
            return;
        }

        NdjsonStream.SetHeaders(ctx);

        try
        {
            var result = await router.RunAsync(
                new AgentRequest(store.Database, config.Identifier, conversationId, body.Prompt, ChannelId: "",
                    body.Parameters ?? new Dictionary<string, string>()),
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                config,
                ctx.RequestAborted);

            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                answer = result.Answer,
                conversationId = result.ConversationId,
            });
        }
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            logger.LogError(e, "Chat stream failed for agentId={AgentId}", body.AgentId);
            try
            {
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Chat stream failed. See server logs for details." });
            }
            catch
            {
            }
        }
    }

    internal sealed class ChatStreamLogger;

    private static async Task WriteBadRequestAsync(HttpContext ctx, string error)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse(error));
    }
}
