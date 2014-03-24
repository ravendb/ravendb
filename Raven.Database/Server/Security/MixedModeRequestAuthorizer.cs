using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security.OAuth;
using Raven.Database.Server.Security.Windows;
using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Security
{
	public class MixedModeRequestAuthorizer : AbstractRequestAuthorizer
	{
		private readonly WindowsRequestAuthorizer windowsRequestAuthorizer = new WindowsRequestAuthorizer();
		private readonly OAuthRequestAuthorizer oAuthRequestAuthorizer = new OAuthRequestAuthorizer();
		private readonly ConcurrentDictionary<string, OneTimeToken> singleUseAuthTokens = new ConcurrentDictionary<string, OneTimeToken>();

		private class OneTimeToken
		{
			private IPrincipal user;
			public string DatabaseName { get; set; }
			public DateTime GeneratedAt { get; set; }
			public IPrincipal User
			{
				get
				{
					return user;
				}
				set
				{
					if (value == null)
					{
						user = null;
						return;
					}
					user = new OneTimetokenPrincipal
					{
						Name = value.Identity.Name
					};
				}
			}
		}

		public class OneTimetokenPrincipal : IPrincipal, IIdentity
		{
			public bool IsInRole(string role)
			{
				return false;
			}

			public IIdentity Identity { get { return this; } }
			public string Name { get; set; }
			public string AuthenticationType { get { return "one-time-token"; } }
			public bool IsAuthenticated { get { return true; } }
		}

		protected override void Initialize()
		{
			windowsRequestAuthorizer.Initialize(database, settings, tenantId, server);
			oAuthRequestAuthorizer.Initialize(database, settings, tenantId, server);
			base.Initialize();
		}

        public bool TryAuthorize(RavenBaseApiController controller, out HttpResponseMessage msg)
		{
			var requestUrl = controller.GetRequestUrl();
			if (NeverSecret.IsNeverSecretUrl(requestUrl))
			{
				msg = controller.GetEmptyMessage();
				return true;
			}

			//CORS pre-flight (ignore creds if using cors).
			if (!String.IsNullOrEmpty(Settings.AccessControlAllowOrigin) && controller.InnerRequest.Method.Method == "OPTIONS")
			{
				msg = controller.GetEmptyMessage();
				return true;
			}

			var oneTimeToken = controller.GetHeader("Single-Use-Auth-Token");
			if (string.IsNullOrEmpty(oneTimeToken) == false)
			{
				return TryAuthorizeSingleUseAuthToken(controller, oneTimeToken, out msg);
			}

			var authHeader = controller.GetHeader("Authorization");
			var hasApiKey = "True".Equals(controller.GetHeader("Has-Api-Key"), StringComparison.CurrentCultureIgnoreCase);
			var hasOAuthTokenInCookie = controller.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.TryAuthorize(controller, hasApiKey, IgnoreDb.Urls.Contains(requestUrl), out msg);
			}
			return windowsRequestAuthorizer.TryAuthorize(controller, IgnoreDb.Urls.Contains(requestUrl), out msg);
		}



        private bool TryAuthorizeSingleUseAuthToken(RavenBaseApiController controller, string token, out HttpResponseMessage msg)
		{
			OneTimeToken value;
			if (singleUseAuthTokens.TryRemove(token, out value) == false)
			{
				msg = controller.GetMessageWithObject(
					new
					{
						Error = "Unknown single use token, maybe it was already used?"
					}, HttpStatusCode.Forbidden);
				return false;
			}

			if (string.Equals(value.DatabaseName, controller.TenantName, StringComparison.InvariantCultureIgnoreCase) == false &&
                (value.DatabaseName == "<system>" && controller.TenantName == null) == false)
			{
				msg = controller.GetMessageWithObject(
					new
					{
						Error = "This single use token cannot be used for this database"
					}, HttpStatusCode.Forbidden);
				return false;
			}
			if ((SystemTime.UtcNow - value.GeneratedAt).TotalMinutes > 2.5)
			{
				msg = controller.GetMessageWithObject(
					new
					{
						Error = "This single use token has expired"
					}, HttpStatusCode.Forbidden);
				return false;
			}

			if (value.User != null)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = value.User.Identity.Name;
			}

			CurrentOperationContext.User.Value = value.User;
			controller.User = value.User;
			msg = controller.GetEmptyMessage();
			return true;
		}

		public IPrincipal GetUser(RavenDbApiController controller)
		{
			var hasApiKey = "True".Equals(controller.GetQueryStringValue("Has-Api-Key"), StringComparison.CurrentCultureIgnoreCase);
			var authHeader = controller.GetHeader("Authorization");
			var hasOAuthTokenInCookie = controller.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetUser(controller, hasApiKey);
			}
			return windowsRequestAuthorizer.GetUser(controller);
		}

		public List<string> GetApprovedDatabases(IPrincipal user, RavenDbApiController controller, string[] databases)
		{
			var authHeader = controller.GetHeader("Authorization");

			List<string> approved;
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
				approved = oAuthRequestAuthorizer.GetApprovedDatabases(user);
			else
				approved = windowsRequestAuthorizer.GetApprovedDatabases(user);

			if (approved.Contains("*"))
				return databases.ToList();

			return approved;
		}

        public List<string> GetApprovedFileSystems(IPrincipal user, RavenDbApiController controller, string[] fileSystems)
        {
            var authHeader = controller.GetHeader("Authorization");

            List<string> approved;
            if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
                approved = oAuthRequestAuthorizer.GetApprovedFileSystems(user);
            else
                approved = windowsRequestAuthorizer.GetApprovedFileSystems(user);

            if (approved.Contains("*"))
                return fileSystems.ToList();

            return approved;
        }

		public override void Dispose()
		{
			windowsRequestAuthorizer.Dispose();
			oAuthRequestAuthorizer.Dispose();
		}

		public string GenerateSingleUseAuthToken(DocumentDatabase db, IPrincipal user)
		{
			var token = new OneTimeToken
			{
				DatabaseName = TenantId,
				GeneratedAt = SystemTime.UtcNow,
				User = user
			};
			var tokenString = Guid.NewGuid().ToString();

			singleUseAuthTokens.TryAdd(tokenString, token);

			if (singleUseAuthTokens.Count > 25)
			{
				foreach (var oneTimeToken in singleUseAuthTokens.Where(x => (x.Value.GeneratedAt - SystemTime.UtcNow).TotalMinutes > 5))
				{
					OneTimeToken value;
					singleUseAuthTokens.TryRemove(oneTimeToken.Key, out value);
				}
			}

			return tokenString;
		}

		public string GenerateSingleUseAuthToken(DocumentDatabase db, IPrincipal user, RavenDbApiController controller)
		{
			var token = new OneTimeToken
			{
				DatabaseName = controller.DatabaseName,
				GeneratedAt = SystemTime.UtcNow,
				User = user
			};
			var tokenString = Guid.NewGuid().ToString();

			singleUseAuthTokens.TryAdd(tokenString, token);

			if (singleUseAuthTokens.Count > 25)
			{
				foreach (var oneTimeToken in singleUseAuthTokens.Where(x => (x.Value.GeneratedAt - SystemTime.UtcNow).TotalMinutes > 5))
				{
					OneTimeToken value;
					singleUseAuthTokens.TryRemove(oneTimeToken.Key, out value);
				}
			}

			return tokenString;
		}
	}
}