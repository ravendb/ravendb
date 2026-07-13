using System.Net.Mail;
using Raven.Quill.Contracts;
using Raven.Quill.Feedback;
using Raven.Quill.Licensing;

namespace Raven.Quill.Endpoints;

public static class SettingsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/settings").WithTags("settings").RequireAuthorization();

        group.MapGet("/license", async (ILicenseStatsProvider provider, CancellationToken token) =>
                Results.Ok(await provider.GetLicenseAsync(token)))
            .WithName("settings.license")
            .Produces<LicenseResponse>();

        group.MapGet("/usage", async (int? year, int? month, ILicenseStatsProvider provider, CancellationToken token) => 
                Results.Ok(await provider.GetUsageAsync(year, month, token)))
            .WithName("settings.usage")
            .Produces<QuillUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);

        group.MapPost("/feedback", SendFeedbackAsync)
            .WithName("settings.feedback")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);
    }

    private static async Task<IResult> SendFeedbackAsync(
        SendFeedbackRequest body,
        IFeedbackSender sender,
        HttpContext context,
        CancellationToken token)
    {
        string name = body.Name?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        string email = body.Email?.Trim() ?? string.Empty;
        if (MailAddress.TryCreate(email, out MailAddress? parsedEmail) == false ||
            string.Equals(parsedEmail.Address, email, StringComparison.OrdinalIgnoreCase) == false)
        {
            return Results.BadRequest(new ApiErrorResponse("email must be a valid email address"));
        }

        string? impression = NormalizeOptional(body.Impression)?.ToLowerInvariant();
        if (impression is not (null or "positive" or "negative"))
            return Results.BadRequest(new ApiErrorResponse("impression must be 'positive', 'negative' or omitted"));

        string message = body.Message?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(message))
            return Results.BadRequest(new ApiErrorResponse("message is required"));

        SendFeedbackRequest request = new(name, email, impression, message, NormalizeOptional(body.StudioView));
        string userAgent = context.Request.Headers.UserAgent.ToString();
        bool wasSent = await sender.SendAsync(request, userAgent, token);

        return wasSent
            ? Results.NoContent()
            : Results.Json(
                new ApiErrorResponse("failed to send feedback"),
                statusCode: StatusCodes.Status502BadGateway);
    }

    private static string? NormalizeOptional(string? value)
    {
        string? trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) ? null : trimmed;
    }
}
