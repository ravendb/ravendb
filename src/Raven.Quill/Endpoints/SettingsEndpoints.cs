using System.Net.Mail;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Logs;
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

        group.MapGet("/logs/configuration", GetLogConfiguration)
            .WithName("settings.logConfiguration")
            .WithDescription("Reports what the live log sinks are doing. A path is null when that sink " +
                             "is off, and auditLogs.level is Off when the audit log is disabled.")
            .Produces<LogConfigurationResponse>();

        group.MapPost("/logs/configuration", UpdateLogConfiguration)
            .WithName("settings.updateLogConfiguration")
            .WithDescription("Changes log levels and the log file path on the running appliance. Send " +
                             "the whole state, not a patch: a logs block with no path switches the file " +
                             "sink off. Changes revert on restart unless persist is true.")
            .Accepts<UpdateLogConfigurationRequest>("application/json")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict)
            .Produces<ApiErrorResponse>(StatusCodes.Status500InternalServerError);

        group.MapGet("/certificates/get", async (IDocumentStore store, int start, int pageSize, CancellationToken token) =>
            {
                var op = new GetCertificatesOperation(start, pageSize);
                var result = await store.Maintenance.Server.SendAsync(op, token);
                return Results.Ok(result.Select(CertificateItem.From).ToArray());
            })
            .Produces<CertificateItem[]>()
            .WithName("settings.certificates")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);

        group.MapPost("/certificates/generate", async (IDocumentStore store, QuillLogger<SettingsLogger> logger, HttpContext ctx, string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance, string? password, CancellationToken token) =>
            {
                var op = new CreateClientCertificateOperation(name, permissions, clearance, password);
                var fileBytes = await store.Maintenance.Server.SendAsync(op, token);

                if (logger.AuditEnabled)
                    logger.Audit("POST",
                        $"Certificate '{name}' clearance={clearance} permissions={{{DescribePermissions(permissions)}}}",
                        ctx);

                return Results.File(fileBytes.RawData, "application/octet-stream", $"{name}_certificates.zip");
            })
            .WithName("settings.certificatesGenerate")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);

        group.MapPost("/certificates/edit", async (IDocumentStore store, QuillLogger<SettingsLogger> logger, HttpContext ctx, string thumbprint, string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance, bool disable, CancellationToken token) =>
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
            })
            .WithName("settings.certificatesEdit")
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
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

    private static IResult GetLogConfiguration(QuillLogging logging) =>
        Results.Ok(new LogConfigurationResponse(
            logging.GetLogsConfiguration(),
            logging.GetAuditLogsConfiguration(),
            logging.GetMicrosoftLogsConfiguration(),
            CanPersist: logging.ConfigPath is not null));

    private static IResult UpdateLogConfiguration(
        UpdateLogConfigurationRequest body,
        QuillLogging logging,
        QuillLogger<SettingsLogger> logger,
        HttpContext ctx)
    {
        if (body is null || body.IsEmpty)
            return Results.BadRequest(new ApiErrorResponse("logs or microsoftLogs is required"));

        if (body.Persist && logging.ConfigPath is null)
        {
            if (logger.AuditEnabled)
                logger.Audit("POST", $"LogConfiguration {Describe(body)} rejected (cannot persist)", ctx);

            return Results.Json(new ApiErrorResponse(
                    "Configuration cannot be persisted because no writable quill.nlog.config is configured."),
                statusCode: StatusCodes.Status409Conflict);
        }

        try
        {
            logging.AssertCanApply(body);
        }
        catch (Exception e) when (e is InvalidOperationException or ArgumentOutOfRangeException)
        {
            return Results.BadRequest(new ApiErrorResponse(e.Message));
        }

        logging.ConfigureLogging(body);

        if (body.Persist)
        {
            try
            {
                QuillNLogFile.Persist(logging);
            }
            catch (Exception e)
            {
                if (logger.IsErrorEnabled)
                    logger.Error(e, "Persisting the log configuration to '{Path}' failed.", logging.ConfigPath);

                if (logger.AuditEnabled)
                    logger.Audit("POST", $"LogConfiguration {Describe(body)} failed to persist", ctx);

                return Results.Json(new ApiErrorResponse(
                        "The log configuration was modified but couldn't be persisted. " +
                        "The configuration will be reverted on restart."),
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        }

        if (logger.AuditEnabled)
            logger.Audit("POST", $"LogConfiguration {Describe(body)} persisted={body.Persist}", ctx);

        return Results.NoContent();
    }

    private static string Describe(UpdateLogConfigurationRequest body)
    {
        var parts = new List<string>();

        if (body.Logs is { } logs)
        {
            parts.Add(string.IsNullOrWhiteSpace(logs.Path) ? "logsPath=(off)" : $"logsPath='{logs.Path}'");
            parts.Add($"minLevel={logs.MinLevel}");
        }

        if (body.MicrosoftLogs is { } microsoftLogs)
            parts.Add($"microsoftMinLevel={microsoftLogs.MinLevel}");

        return string.Join(" ", parts);
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
