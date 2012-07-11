using System;
using System.ComponentModel.Composition;
using System.Net;
using System.Text;
using System.Threading;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthClientCredentialsTokenResponder : AbstractRequestResponder
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

		private int numberOfTokensIssued;
		public int NumberOfTokensIssued
		{
			get { return numberOfTokensIssued; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.Request.Headers["Accept"] != TokenContentType)
			{
				context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
				context.WriteJson(new { error = "invalid_request", error_description = "Accept should be: " + TokenContentType });

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
				context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
				context.Response.AddHeader("WWW-Authenticate", "Basic realm=\"Raven DB\"");
				context.WriteJson(new { error = "invalid_client", error_description = "No client authentication was provided" });

				return;
			}

			AccessTokenBody.DatabaseAccess[] authorizedDatabases;
			if (!AuthenticateClient.Authenticate(Database, identity.Item1, identity.Item2, out authorizedDatabases))
			{
				if ((Database == SystemDatabase ||
				     !AuthenticateClient.Authenticate(SystemDatabase, identity.Item1, identity.Item2, out authorizedDatabases)))
				{
					context.Response.StatusCode = (int) HttpStatusCode.Unauthorized;
					context.Response.AddHeader("WWW-Authenticate", "Basic realm=\"Raven DB\"");
					context.WriteJson(new { error = "unauthorized_client", error_description = "Invalid client credentials" });

					return;
				}
			}

			Interlocked.Increment(ref numberOfTokensIssued);

			var userId = identity.Item1;

			var token = AccessToken.Create(Settings.OAuthTokenCertificate, new AccessTokenBody
			{
				UserId = userId,
				AuthorizedDatabases = authorizedDatabases
			});

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
			if (string.IsNullOrEmpty(auth) || auth.StartsWith("Basic", StringComparison.InvariantCultureIgnoreCase) == false)
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