using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace Raven.Server.Web.Authentication;

public class TwoFactorAuthenticationHandler : ServerRequestHandler
{
    [RavenAction("/authentication/2fa", "POST", AuthorizationStatus.UnauthenticatedClients)]
    public async Task ValidateTotp()
    {
        using var _ = ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx);
        ctx.OpenReadTransaction();

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
            Server.RegisterTwoFactorAuthSuccess(clientCert.Thumbprint, period);
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Accepted;
        }
        else
        {
            ReplyWith(ctx, $"Wrong token provided for {clientCert.Thumbprint} ({clientCert.FriendlyName})", HttpStatusCode.NotAcceptable);
        }
    }

    private void ReplyWith(TransactionOperationContext ctx, string err, HttpStatusCode httpStatusCode)
    {
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
