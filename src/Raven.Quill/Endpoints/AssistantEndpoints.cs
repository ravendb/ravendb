using Microsoft.AspNetCore.Http.Features;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;

namespace Raven.Quill.Endpoints;

public static class AssistantEndpoints
{
    private const string EventStreamContentType = "text/event-stream";

    public static void Map(WebApplication app)
    {
        app.MapPost("/api/assistant/chat", ChatAsync)
            .RequireAuthorization()
            .WithTags("assistant")
            .WithName("assistant.chat")
            // Relays the AI service's Server-Sent Events, so the response is not a JSON body and the
            // service's own codes (401 with a Status body, 413, 429) reach the client unchanged.
            .Produces(StatusCodes.Status200OK, contentType: EventStreamContentType)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);

        app.MapGet("/api/assistant/consent", CheckConsentAsync)
            .RequireAuthorization()
            .WithTags("assistant")
            .WithName("assistant.consent")
            .Produces<AssistantConsentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);

        app.MapPost("/api/assistant/consent", GiveConsentAsync)
            .RequireAuthorization()
            .WithTags("assistant")
            .WithName("assistant.giveConsent")
            .Produces<AssistantConsentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);
    }

    private static async Task<IResult> CheckConsentAsync(IAiHelperClient aiClient, CancellationToken ct) =>
        ToConsentResult(await aiClient.CheckConsentAsync(ct));

    // Consent is the operator's to give: this runs only because they accepted the AI service's terms in
    // the assistant panel, and nothing else in Quill grants it for them.
    private static async Task<IResult> GiveConsentAsync(
        IAiHelperClient aiClient,
        ILogger<AssistantLogger> logger,
        CancellationToken ct)
    {
        var status = await aiClient.GiveConsentAsync(ct);
        if (status == AiHelperStatus.Success)
            logger.LogInformation("AI assistant consent granted for this appliance's license.");

        return ToConsentResult(status);
    }

    private static IResult ToConsentResult(AiHelperStatus status) =>
        status is AiHelperStatus.Success or AiHelperStatus.ConsentRequired or AiHelperStatus.InvalidCredentials
            ? Results.Ok(new AssistantConsentResponse(status))
            : Results.Json(
                new ApiErrorResponse("The AI service could not be reached."),
                statusCode: StatusCodes.Status502BadGateway);

    // The response is relayed rather than translated, mirroring Raven.Server's own /assistant/assist
    // processor: the result frame carries more than the answer (follow-up questions, usage,
    // endpoints), and reshaping it here would mean re-modelling that contract inside Quill and
    // dropping whatever a given build has not heard of. Message size is the service's to police as
    // well — it answers RequestTooLarge.
    //
    // The request, by contrast, is deliberately narrowed to what this panel can produce. The service
    // also accepts ActionsResponses and AdditionalAttachedContext, which drive the Studio's
    // endpoint-calling turns; until Quill has a UI for approving those calls there is nothing to send
    // back, so a turn that answers with Endpoints goes unanswered.
    private static async Task ChatAsync(
        HttpContext ctx,
        AssistantChatRequest body,
        IAiHelperClient aiClient,
        ILogger<AssistantLogger> logger,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(body?.Message))
        {
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("message is required"), ct);
            return;
        }

        try
        {
            using var upstream = await aiClient.SendChatAsync(
                body.Message,
                string.IsNullOrWhiteSpace(body.ConversationId) ? null : body.ConversationId,
                ct);

            ctx.Response.StatusCode = (int)upstream.StatusCode;
            var contentType = upstream.Content.Headers.ContentType?.ToString();
            if (contentType is not null)
                ctx.Response.ContentType = contentType;

            if (upstream.IsSuccessStatusCode && IsEventStream(contentType))
            {
                ctx.Response.Headers["Cache-Control"] = "no-cache";
                ctx.Response.Headers["X-Accel-Buffering"] = "no";
                // Chunks have to reach the browser as they arrive, not once the answer is finished.
                ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
            }

            await upstream.Content.CopyToAsync(ctx.Response.Body, ct);
        }
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            logger.LogError(e, "AI assistant chat failed.");

            // Once the relay has started there is no way left to say so, and the client reads the
            // missing Done frame as the failure.
            if (ctx.Response.HasStarted)
                return;

            ctx.Response.StatusCode = StatusCodes.Status502BadGateway;
            // Quill's own error shape rather than the AI service's: WriteAsJsonAsync serializes through
            // the app's camelCase policy, so a hand-rolled { Status = ... } would arrive as "status"
            // and never match the PascalCase Status the client reads out of relayed refusals.
            await ctx.Response.WriteAsJsonAsync(
                new ApiErrorResponse("The AI assistant is unavailable right now."), ct);
        }
    }

    private static bool IsEventStream(string? contentType) =>
        contentType?.StartsWith(EventStreamContentType, StringComparison.OrdinalIgnoreCase) == true;

    internal sealed class AssistantLogger;
}
