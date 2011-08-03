using System;
using System.ComponentModel.Composition;
using System.Net;
using System.Text;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Security.OAuth
{
    public class OAuthClientCredentialsTokenEndpoint : AbstractRequestResponder
    {
        const string TokenContentType = "application/json;charset=UTF-8";
        const string TokenGrantType = "client_credentials";

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

            if (context.Request.Headers["Content-Type"] != TokenContentType)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "invalid_request", error_description = "Content-Type should be: " + TokenContentType });

                return;
            }

            if (context.Request.Headers["grant_type"] != TokenGrantType)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "unsupported_grant_type", error_description = "Only supported grant_type is: " + TokenGrantType });

                return;
            }

			var identity = GetUserAndPassword(context);

			if (identity == null)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                context.WriteJson(new { error = "invalid_client", error_description = "No client authentication was provided" });

                return;
            }

        	if (!AuthenticateClient.Authenticate(identity.Item1, identity.Item2))
        	{
        		context.Response.StatusCode = (int) HttpStatusCode.BadRequest;
        		context.WriteJson(new {error = "unauthorized_client", error_description = "Invalid client credentials"});

        		return;
        	}

        	var userId = identity.Item1;
        	var authorizedDatabases = new[] {"*"};

        	var token = AccessToken.Create(Settings.OAuthTokenCertificatePath, Settings.OAuthTokenCertificatePassword, userId,
        	                               authorizedDatabases);

        	context.Write(token.Serialize());
        }

    	private static Tuple<string, string> GetUserAndPassword(IHttpContext context)
    	{
			if (context.User != null)
			{
				var httpListenerBasicIdentity = context.User.Identity as HttpListenerBasicIdentity;
				if (httpListenerBasicIdentity != null)
				{
					return Tuple.Create(httpListenerBasicIdentity.Name, httpListenerBasicIdentity.Password);
				}
			}

    		var auth = context.Request.Headers["Authorization"];
			if(string.IsNullOrEmpty(auth) || auth.StartsWith("Basic",StringComparison.InvariantCultureIgnoreCase) == false)
			{
				return null;
			}

    		var userAndPass = Encoding.UTF8.GetString(Convert.FromBase64String(auth.Substring("Basic ".Length)));
    		var parts = userAndPass.Split(':');
			if (parts.Length != 2)
				return null;

    		return Tuple.Create(parts[0], parts[1]);
    	}
    }
}