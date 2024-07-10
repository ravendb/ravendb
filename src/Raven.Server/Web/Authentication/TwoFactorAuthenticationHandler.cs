using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Config.Settings;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Web.Authentication;

public class TwoFactorAuthenticationHandler : ServerRequestHandler
{
    [RavenAction("/authentication/2fa/configuration", "GET", AuthorizationStatus.UnauthenticatedClients)]
    public async Task TotpConfiguration()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(TotpServerConfiguration.DefaultTwoFactorSessionDurationInMin));
                writer.WriteInteger(Server.Configuration.Security.DefaultTwoFactorSessionDuration.GetValue(TimeUnit.Minutes));
                writer.WriteComma();
                writer.WritePropertyName(nameof(TotpServerConfiguration.MaxTwoFactorSessionDurationInMin));
                writer.WriteInteger(Server.Configuration.Security.MaxTwoFactorSessionDuration.GetValue(TimeUnit.Minutes));
                writer.WriteEndObject();
            }
        }
    }

    [RavenAction("/authentication/2fa", "DELETE", AuthorizationStatus.UnauthenticatedClients)]
    public Task LogoutTotp()
    {
        var feature = (RavenServer.AuthenticateConnection)HttpContext.Features.Get<IHttpAuthenticationFeature>();
        var authRegistration = feature.TwoFactorAuthRegistration;
        if (authRegistration != null)
            Server.TwoFactor.ForgotTwoFactorAuthSuccess(authRegistration);
        
        NoContentStatus();
        return Task.CompletedTask;
    }

    [RavenAction("/authentication/2fa", "POST", AuthorizationStatus.UnauthenticatedClients)]
    public async Task ValidateTotp()
    {
        using var _ = ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx);
        ctx.OpenReadTransaction();

        bool hasLimits = GetBoolValueQueryString("hasLimits", false) ?? true;
        var sessionDuration = GetIntValueQueryString("sessionDurationInMin", false) 
                              ?? Server.Configuration.Security.DefaultTwoFactorSessionDuration.GetValue(TimeUnit.Minutes);

        var maxSessionDuration = Server.Configuration.Security.MaxTwoFactorSessionDuration.GetValue(TimeUnit.Minutes);
        if (sessionDuration > maxSessionDuration)
        {
            await ReplyWith(ctx, $"Requested two factor authentication duration is higher than maximum session length ({maxSessionDuration})", HttpStatusCode.BadRequest);
            return;
        }
        

        var clientCert = GetCurrentCertificate();

        if (clientCert == null)
        {
            await ReplyWith(ctx, "Two factor authentication requires that you'll use a client certificate, but none was provided.", HttpStatusCode.BadRequest);
            return;
        }

        using var input = await ctx.ReadForMemoryAsync(RequestBodyStream(), "2fa-auth");
        
        var certificate = ServerStore.Cluster.GetCertificateByThumbprint(ctx, clientCert.Thumbprint);
        if (certificate == null)
        {
            await ReplyWith(ctx, $"The certificate {clientCert.Thumbprint} ({clientCert.FriendlyName}) is not known to the server", HttpStatusCode.BadRequest);
            return;
        }

        if (certificate.TryGet(nameof(PutCertificateCommand.TwoFactorAuthenticationKey), out string key) == false)
        {
            await ReplyWith(ctx, $"The certificate {clientCert.Thumbprint} ({clientCert.FriendlyName}) is not set up for two factor authentication", HttpStatusCode.BadRequest);
            return;
        }

        input.TryGet("Token", out int token);

        if (TwoFactorAuthentication.ValidateCode(key, token))
        {
            var period = TimeSpan.FromMinutes(sessionDuration);
            
            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                LogAuditFor(nameof(TwoFactorAuthenticationHandler), "AUTH", $"2FA session open for {period} (until: {DateTime.UtcNow.Add(period)}). Has limits: {hasLimits}");
            }

            string expectedCookieValue = null;
            
            if (hasLimits)
            {
                expectedCookieValue = Guid.NewGuid().ToString();
            
                HttpContext.Response.Cookies.Append(TwoFactorAuthentication.CookieName, expectedCookieValue, new CookieOptions
                {
                    HttpOnly = true,
                    MaxAge = period,
                    IsEssential = true,
                    SameSite = SameSiteMode.Strict,
                    Secure = true
                });
            }

            TwoFactor.TwoFactorAuthRegistration twoFactorAuthRegistration = new()
            {
                Thumbprint = clientCert.Thumbprint,
                Period = period,
                HasLimits = hasLimits,
                ExpectedCookieValue = expectedCookieValue
            };

            Server.TwoFactor.RegisterTwoFactorAuthSuccess(twoFactorAuthRegistration);
            
            var feature = (RavenServer.AuthenticateConnection)HttpContext.Features.Get<IHttpAuthenticationFeature>();
            feature.SuccessfulTwoFactorAuthentication(); // enable access for the current connection
            
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Accepted;
            HttpContext.Response.Headers.Remove("Content-Type");
        }
        else
        {
            await ReplyWith(ctx, $"Wrong token provided for {clientCert.Thumbprint} ({clientCert.FriendlyName})", HttpStatusCode.NotAcceptable);
        }
    }

    private async Task ReplyWith(TransactionOperationContext ctx, string err, HttpStatusCode httpStatusCode)
    {
        if (LoggingSource.AuditLog.IsInfoEnabled)
        {
            LogAuditFor(nameof(TwoFactorAuthenticationHandler), "AUTH", $"2FA failure: {err}");
        }

        HttpContext.Response.StatusCode = (int)httpStatusCode;
        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Error");
            writer.WriteString(err);
            writer.WriteEndObject();
        }
    }


    public class TotpServerConfiguration
    {
        public int DefaultTwoFactorSessionDurationInMin;
        public int MaxTwoFactorSessionDurationInMin;
    }
}
