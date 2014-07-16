using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Principal;
using Mono.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
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
			readonly Stopwatch age = Stopwatch.StartNew();

			private IPrincipal user;
			public string DatabaseName { get; set; }
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
			public TimeSpan Age
			{
				get { return age.Elapsed; }
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
			if (value.Age.TotalMinutes > 2.5) // if the value is over 2.5 minutes old, reject it
			{
				context.SetStatusToForbidden();
				context.WriteJson(new
				{
					Error = "This single use token has expired after " + value.Age.TotalSeconds + " seconds"
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
				User = user
			};
			var tokenString = Guid.NewGuid().ToString();

			singleUseAuthTokens.TryAdd(tokenString, token);

			if (singleUseAuthTokens.Count > 25)
			{
				foreach (var oneTimeToken in singleUseAuthTokens.Where(x => x.Value.Age.TotalMinutes > 3))
				{
					OneTimeToken value;
					singleUseAuthTokens.TryRemove(oneTimeToken.Key, out value);
				}
			}

			return tokenString;
		}
	}
}