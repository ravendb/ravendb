using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.Extensions.Primitives;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;

namespace Raven.Server.Web.Authentication;

public class TwoFactorAuthenticationHandler : ServerRequestHandler
{
    private readonly Logger _auditLogger;

    public TwoFactorAuthenticationHandler()
    {
        _auditLogger = LoggingSource.AuditLog.GetLogger(nameof(TwoFactorAuthenticationHandler), "Audit");
    }

    [RavenAction("/authentication/2fa", "POST", AuthorizationStatus.UnauthenticatedClients)]
    public async Task ValidateTotp()
    {        
        using var _ = ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx);
        ctx.OpenReadTransaction();

        bool hasLimits = GetBoolValueQueryString("hasLimits") ?? false;
        var ipsStrVals = GetStringValuesQueryString("ip");
        var ips = ipsStrVals.Count == 0 ? new[] { HttpContext.Connection.RemoteIpAddress?.ToString() } : ipsStrVals.ToArray();

        var clientCert = GetCurrentCertificate();

        if (clientCert == null)
        {
            ReplyWith(ctx, "Two factor authentication requires that you'll use a client certificate, but none was provided.", HttpStatusCode.BadRequest);
            return;
        }

        using var input = await ctx.ReadForMemoryAsync(RequestBodyStream(), "2fa-auth");
        
        var certificate = ServerStore.Cluster.GetCertificateByThumbprint(ctx, clientCert.Thumbprint);
        if (certificate == null)
        {
            ReplyWith(ctx, $"The certificate {clientCert.Thumbprint} ({clientCert.FriendlyName}) is not known to the server", HttpStatusCode.BadRequest);
            return;
        }

        if (certificate.TryGet(nameof(PutCertificateCommand.TwoFactorAuthenticationKey), out string key) == false)
        {
            ReplyWith(ctx, $"The certificate {clientCert.Thumbprint} ({clientCert.FriendlyName}) is not set up for two factor authentication", HttpStatusCode.BadRequest);
            return;
        }


        input.TryGet("Token", out int token);

        if (TwoFactorAuthentication.ValidateCode(key, token))
        {
            if (certificate.TryGet(nameof(PutCertificateCommand.TwoFactorAuthenticationValidityPeriod), out TimeSpan period) == false)
            {
                period = TimeSpan.FromHours(2);
            }
            var feature = (RavenServer.AuthenticateConnection)HttpContext.Features.Get<IHttpAuthenticationFeature>();
            feature.SuccessfulTwoFactorAuthentication(); // enable access for the current connection
            
            if (_auditLogger.IsInfoEnabled)
            {
                _auditLogger.Info($"Connection from {HttpContext.Connection.RemoteIpAddress} with new certificate '{clientCert.Subject} ({clientCert.Thumbprint})' successfully authenticated with two factor auth for {period}. Has limits: {hasLimits}, IPs: [{string.Join(", ", ips)}]");
            }


            string expectedCookieValue = Guid.NewGuid().ToString();
            string csrfAccessToken = Guid.NewGuid().ToString();
            
            HttpContext.Response.Cookies.Append(TwoFactorAuthentication.CookieName, expectedCookieValue, new CookieOptions
            {
                HttpOnly = true,
                MaxAge = period,
                IsEssential = true,
                SameSite = SameSiteMode.Strict,
                Secure = true
            });
            
            Server.RegisterTwoFactorAuthSuccess(new RavenServer.TwoFactorAuthRegistration
            {
                Thumbprint = clientCert.Thumbprint,
                Period = period,
                IpAddresses = ips,
                HasLimits = hasLimits,
                CsrfAccessToken = csrfAccessToken,
                ExpectedCookieValue = expectedCookieValue
            });
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Accepted;
            using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Token");
                writer.WriteString(csrfAccessToken);
                writer.WriteEndObject();
            }
        }
        else
        {
            ReplyWith(ctx, $"Wrong token provided for {clientCert.Thumbprint} ({clientCert.FriendlyName})", HttpStatusCode.NotAcceptable);
        }
    }

    private void ReplyWith(TransactionOperationContext ctx, string err, HttpStatusCode httpStatusCode)
    {
        if (_auditLogger.IsInfoEnabled)
        {
            var clientCert = GetCurrentCertificate();
            _auditLogger.Info(
                $"Two factor auth failure from IP: {HttpContext.Connection.RemoteIpAddress}  with cert: '{clientCert?.Thumbprint ?? "None"}/{clientCert?.Subject ?? "None"}' because: {err}");
        }
        HttpContext.Response.StatusCode = (int)httpStatusCode;
        using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Error");
            writer.WriteString(err);
            writer.WriteEndObject();
        }
    }
}
