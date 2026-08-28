using System.Net.Mail;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Quill.Contracts;
using Raven.Quill.Feedback;
using Raven.Quill.Licensing;
using Raven.Quill.Logging;

namespace Raven.Quill.Endpoints;

public static class SettingsEndpoints
{
    private const int MaxNameLength = 256;
    private const int MaxEmailLength = 254; // RFC 5321 address limit
    private const int MaxMessageLength = 8_192;
    private const int MaxStudioViewLength = 512;

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/settings").WithTags("settings").RequireAuthorization();

        group.MapGet("/license", async (ILicenseStatsProvider provider, CancellationToken token) =>
                Results.Ok(await provider.GetLicenseAsync(token)))
            .WithName("settings.license")
            .Produces<LicenseResponse>();

        group.MapGet("/usage", async (ILicenseStatsProvider provider, int year, int? month, int? day, CancellationToken token) =>
                Results.Ok(await provider.GetUsageAsync(year, month, day, token)))
            .WithName("settings.usage")
            .Produces<QuillUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);

        group.MapPost("/feedback", SendFeedbackAsync)
            .WithName("settings.feedback")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);

        group.MapGet("/certificates/get", (IDocumentStore store, int start, int pageSize, QuillLogger<SettingsLogger> logger, CancellationToken token) =>
                GuardCertificateErrorsAsync(logger, async () =>
                {
                    var op = new GetCertificatesOperation(start, pageSize);
                    var result = await store.Maintenance.Server.SendAsync(op, token);
                    return Results.Ok(result.Select(CertificateItem.From).ToArray());
                }))
            .Produces<CertificateItem[]>()
            .WithName("settings.certificates")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);

        group.MapPost("/certificates/generate", (IDocumentStore store, GenerateClientCertificateRequest body, QuillLogger<SettingsLogger> logger, HttpContext ctx, CancellationToken token) =>
                GuardCertificateErrorsAsync(logger, async () =>
                {
                    var op = new CreateClientCertificateOperation(body.Name, body.Permissions, body.Clearance, body.Password);
                    var fileBytes = await store.Maintenance.Server.SendAsync(op, token);

                    if (logger.AuditEnabled)
                        logger.Audit("POST",
                            $"Certificate '{body.Name}' clearance={body.Clearance} " +
                            $"permissions={{{DescribePermissions(body.Permissions)}}}",
                            ctx);

                    return Results.File(fileBytes.RawData, "application/octet-stream", $"{body.Name}_certificates.zip");
                }))
            .WithName("settings.certificatesGenerate")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status403Forbidden);

        group.MapPost("/certificates/edit", (IDocumentStore store, string thumbprint, string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance, bool disable, QuillLogger<SettingsLogger> logger, HttpContext ctx, CancellationToken token) =>
                GuardCertificateErrorsAsync(logger, async () =>
                {
                    var existing = await store.Maintenance.Server.SendAsync(new GetCertificateOperation(thumbprint), token);
                    if (existing is null)
                        return Results.NotFound(new ApiErrorResponse($"no certificate with thumbprint '{thumbprint}'"));

                    var op = new EditClientCertificateOperation(new EditClientCertificateOperation.Parameters
                    {
                        Thumbprint = thumbprint,
                        Permissions = permissions,
                        Disabled = disable,
                        Name = name,
                        Clearance = clearance
                    });
                    await store.Maintenance.Server.SendAsync(op, token);

                    if (logger.AuditEnabled)
                        logger.Audit("POST",
                            $"Certificate '{name}' thumbprint={thumbprint} clearance={clearance} disabled={disable} " +
                            $"permissions={{{DescribePermissions(permissions)}}}",
                            ctx);

                    return Results.Ok();
                }))
            .WithName("settings.certificatesEdit")
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status403Forbidden);
    }

    private static async Task<IResult> GuardCertificateErrorsAsync(QuillLogger<SettingsLogger> logger, Func<Task<IResult>> action)
    {
        try
        {
            return await action();
        }
        catch (LicenseLimitException ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, "certificate operation rejected by license");
            var error = ex.LimitType switch
            {
                LimitType.ReadOnlyCertificates =>
                    "your license does not include read-only certificates; grant at least one app ReadWrite access, or upgrade the license",
                LimitType.InvalidLicense =>
                    "the RavenDB license is in an invalid state, so certificates cannot be issued",
                _ => "the RavenDB license does not allow this certificate operation",
            };
            return Results.Json(new ApiErrorResponse(error), statusCode: StatusCodes.Status403Forbidden);
        }
        catch (RavenException ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex, "certificate operation rejected by RavenDB");
            return Results.BadRequest(new ApiErrorResponse("certificate request rejected; see server logs for details"));
        }
    }

    public record CertificateItem(
        string Name,
        SecurityClearance SecurityClearance,
        string Thumbprint,
        DateTime? NotAfter,
        DateTime? NotBefore,
        Dictionary<string, DatabaseAccess> Permissions,
        bool Disabled)
    {
        public static CertificateItem From(CertificateDefinition source) => new(
            source.Name,
            source.SecurityClearance,
            source.Thumbprint,
            source.NotAfter,
            source.NotBefore,
            new Dictionary<string, DatabaseAccess>(source.Permissions, StringComparer.OrdinalIgnoreCase),
            source.Disabled);
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
        if (name.Length > MaxNameLength)
            return Results.BadRequest(new ApiErrorResponse($"name must be {MaxNameLength} characters or fewer"));

        string email = body.Email?.Trim() ?? string.Empty;
        if (email.Length > MaxEmailLength ||
            MailAddress.TryCreate(email, out MailAddress? parsedEmail) == false ||
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
        if (message.Length > MaxMessageLength)
            return Results.BadRequest(new ApiErrorResponse($"message must be {MaxMessageLength} characters or fewer"));

        string? studioView = NormalizeOptional(body.StudioView);
        if (studioView?.Length > MaxStudioViewLength)
            return Results.BadRequest(new ApiErrorResponse($"studioView must be {MaxStudioViewLength} characters or fewer"));

        SendFeedbackRequest request = new(name, email, impression, message, studioView);
        string userAgent = context.Request.Headers.UserAgent.ToString();
        bool wasSent = await sender.SendAsync(request, userAgent, token);

        return wasSent
            ? Results.NoContent()
            : Results.Json(
                new ApiErrorResponse("failed to send feedback"),
                statusCode: StatusCodes.Status502BadGateway);
    }

    private static string DescribePermissions(Dictionary<string, DatabaseAccess>? permissions) =>
        permissions is null || permissions.Count == 0
            ? string.Empty
            : string.Join(", ", permissions
                .OrderBy(permission => permission.Key, StringComparer.Ordinal)
                .Select(permission => $"{permission.Key}:{permission.Value}"));

    private static string? NormalizeOptional(string? value)
    {
        string? trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) ? null : trimmed;
    }

    internal sealed class SettingsLogger;
}
