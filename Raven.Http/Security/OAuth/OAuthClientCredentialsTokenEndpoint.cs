using System;
using System.ComponentModel.Composition;
using System.Net;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Security.OAuth
{
    public class OAuthClientCredentialsTokenEndpoint : AbstractRequestResponder
    {
        const string tokenContentType = "application/json;charset=UTF-8";
        const string tokenGrantType = "client_credentials";

        [Import]
        public IAuthenticateClient AuthenticateClient { get; set; }

        public override string UrlPattern
        {
            get { return @"^/OAuth/AccessToken$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            if (this.Settings.AuthenticationMode != "OAuth")
            {
                context.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (context.Request.Headers["Content-Type"] != tokenContentType)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "invalid_request", error_description = "Content-Type should be: " + tokenContentType });

                return;
            }

            if (context.Request.Headers["grant_type"] != tokenGrantType)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "unsupported_grant_type", error_description = "Only supported grant_type is: " + tokenGrantType });

                return;
            }

            var user = context.User;

            if (user == null || !(user.Identity is HttpListenerBasicIdentity))
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "invalid_client", error_description = "No client authentication was provided" });

                return;
            }
            
            var identity = (HttpListenerBasicIdentity)context.User.Identity;
            var clientId = identity.Name;
            var clientSecret = identity.Password;

            if (!AuthenticateClient.Authenticate(clientId, clientSecret))
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "unauthorized_client", error_description = "Invalid client credentials" });

                return;
            }

            //TODO: Add userId lookup from client credentials
            var userId = "";
            var authorizedDatabases = new[] { "*" };

            var token = AccessToken.Create(Settings.OAuthTokenCertificatePath, Settings.OAuthTokenCertificatePassword, userId, authorizedDatabases);

            context.Write(token.Serialize());
        }
    }
}