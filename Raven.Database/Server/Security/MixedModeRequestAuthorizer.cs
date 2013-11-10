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

		public bool Authorize(IHttpContext context)
		{
			var requestUrl = context.GetRequestUrl();
			if (NeverSecret.Urls.Contains(requestUrl))
				return true;

			//CORS pre-flight (ignore creds if using cors).
			if (!String.IsNullOrEmpty(Settings.AccessControlAllowOrigin) && context.Request.HttpMethod == "OPTIONS")
			{ return true; }

			var oneTimeToken = context.Request.Headers["Single-Use-Auth-Token"];
			if (string.IsNullOrEmpty(oneTimeToken) == false)
			{
				return AuthorizeUsingleUseAuthToken(context, oneTimeToken);
			}

			var authHeader = context.Request.Headers["Authorization"];
			var hasApiKey = "True".Equals(context.Request.Headers["Has-Api-Key"], StringComparison.CurrentCultureIgnoreCase);
			var hasOAuthTokenInCookie = context.Request.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.Authorize(context, hasApiKey, IgnoreDb.Urls.Contains(requestUrl));
			}
			return windowsRequestAuthorizer.Authorize(context, IgnoreDb.Urls.Contains(requestUrl));
		}

		public bool TryAuthorize(RavenApiController controller, out HttpResponseMessage msg)
		{
			var requestUrl = controller.GetRequestUrl();
			if (NeverSecret.Urls.Contains(requestUrl))
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
				return TryAuthorizeUsingleUseAuthToken(controller, oneTimeToken, out msg);
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

		private bool AuthorizeUsingleUseAuthToken(IHttpContext context, string token)
		{
			OneTimeToken value;
			if (singleUseAuthTokens.TryRemove(token, out value) == false)
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "Unknown single use token, maybe it was already used?"
				});
				return false;
			}
			if (string.Equals(value.DatabaseName, TenantId, StringComparison.InvariantCultureIgnoreCase) == false)
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "This single use token cannot be used for this database"
				});
				return false;
			}
			if ((SystemTime.UtcNow - value.GeneratedAt).TotalMinutes > 2.5)
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "This single use token has expired"
				});
				return false;
			}

			if (value.User != null)
			{
				CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = value.User.Identity.Name;
			}
			CurrentOperationContext.User.Value = value.User;
			context.User = value.User;
			return true;
		}

		private bool TryAuthorizeUsingleUseAuthToken(RavenApiController controller, string token, out HttpResponseMessage msg)
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

			if (string.Equals(value.DatabaseName, controller.DatabaseName, StringComparison.InvariantCultureIgnoreCase) == false &&
				(value.DatabaseName == "<system>" && controller.DatabaseName == null) == false)
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

		public IPrincipal GetUser(IHttpContext context)
		{
			var hasApiKey = "True".Equals(context.Request.Headers["Has-Api-Key"], StringComparison.CurrentCultureIgnoreCase);
			var authHeader = context.Request.Headers["Authorization"];
			var hasOAuthTokenInCookie = context.Request.HasCookie("OAuth-Token");
			if (hasApiKey || hasOAuthTokenInCookie ||
				string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				return oAuthRequestAuthorizer.GetUser(context, hasApiKey);
			}
			return windowsRequestAuthorizer.GetUser(context);
		}

		public IPrincipal GetUser(RavenApiController controller)
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

		public List<string> GetApprovedDatabases(IPrincipal user, IHttpContext context, string[] databases)
		{
			var authHeader = context.Request.Headers["Authorization"];
			List<string> approved;
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
				approved = oAuthRequestAuthorizer.GetApprovedDatabases(user);
			else
				approved = windowsRequestAuthorizer.GetApprovedDatabases(user);

			if (approved.Contains("*"))
				return databases.ToList();

			return approved;
		}

		public List<string> GetApprovedDatabases(IPrincipal user, RavenApiController controller, string[] databases)
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

		public string GenerateSingleUseAuthToken(DocumentDatabase db, IPrincipal user, RavenApiController controller)
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