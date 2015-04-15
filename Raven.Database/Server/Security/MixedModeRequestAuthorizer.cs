using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security.OAuth;
using Raven.Database.Server.Security.Windows;
using System.Linq;

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
			public string ResourceName { get; set; }
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
						Name = value.Identity.Name,
                        IsAdministratorInAnonymouseMode = value.IsAdministrator(AnonymousUserAccessMode.None)
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
                if (role == "Administrators")
                {
                    return IsAdministratorInAnonymouseMode;
                }
			    return false;
			}

            public bool IsAdministratorInAnonymouseMode { get; set; }
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
			if (Settings.AccessControlAllowOrigin.Count > 0 && controller.InnerRequest.Method.Method == "OPTIONS")
			{
				msg = controller.GetEmptyMessage();
				return true;
			}

			var oneTimeToken = controller.GetHeader("Single-Use-Auth-Token");
            if (string.IsNullOrEmpty(oneTimeToken))
			{
			    oneTimeToken = controller.GetQueryStringValue("singleUseAuthToken");
			}

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

	    public bool TryAuthorizeSingleUseAuthToken(string token, string tenantName, out object msg, out HttpStatusCode statusCode, out IPrincipal user)
		{
            user = null;
			OneTimeToken value;
			if (singleUseAuthTokens.TryRemove(token, out value) == false)
			{
                msg = new
				{
					Error = "Unknown single use token, maybe it was already used?"
                };
                statusCode = HttpStatusCode.Forbidden;
				return false;
			}

			if (string.Equals(value.ResourceName, tenantName, StringComparison.InvariantCultureIgnoreCase) == false &&
                (value.ResourceName == Constants.SystemDatabase && tenantName == null) == false)
			{
                msg = new
				{
					Error = "This single use token cannot be used for this resource!"
                };
                statusCode = HttpStatusCode.Forbidden;
				return false;
			}

			if (value.Age.TotalMinutes > 2.5) // if the value is over 2.5 minutes old, reject it
			{
                msg = new
				{
					Error = "This single use token has expired after " + value.Age.TotalSeconds + " seconds"
                };
                statusCode = HttpStatusCode.Forbidden;
				return false;
			}

	        msg = null;
	        statusCode = HttpStatusCode.OK;

            CurrentOperationContext.User.Value = user = value.User;
			return true;
		}

        private bool TryAuthorizeSingleUseAuthToken(RavenBaseApiController controller, string token, out HttpResponseMessage msg)
		{
            if (controller.WasAlreadyAuthorizedUsingSingleAuthToken)
            {
                msg = controller.GetEmptyMessage();
                return true;
            }

            object result;
            HttpStatusCode statusCode;
            IPrincipal user;
            var success = TryAuthorizeSingleUseAuthToken(token, controller.TenantName, out result, out statusCode, out user);
            controller.User = user;
            msg = success == false ? controller.GetMessageWithObject(result, statusCode) : controller.GetEmptyMessage();

            controller.WasAlreadyAuthorizedUsingSingleAuthToken = success;
            return success;
        }

	    public IPrincipal GetUser(RavenDbApiController controller)
		{
            if (controller.WasAlreadyAuthorizedUsingSingleAuthToken)
            {
                return controller.User;
            }

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

		public List<string> GetApprovedResources(IPrincipal user, RavenDbApiController controller, string[] databases)
		{
			var authHeader = controller.GetHeader("Authorization");

			List<string> approved;
			if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
				approved = oAuthRequestAuthorizer.GetApprovedResources(user);
			else
				approved = windowsRequestAuthorizer.GetApprovedResources(user);

			if (approved.Contains("*"))
				return databases.ToList();

			return approved;
		}

        public List<string> GetApprovedResources(IPrincipal user, string authHeader, string[] databases)
        {
            List<string> approved;
            if (string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
                approved = oAuthRequestAuthorizer.GetApprovedResources(user);
            else
                approved = windowsRequestAuthorizer.GetApprovedResources(user);

            if (approved.Contains("*"))
                return databases.ToList();

            return approved;
        }

		public override void Dispose()
		{
			windowsRequestAuthorizer.Dispose();
			oAuthRequestAuthorizer.Dispose();
		}

		public string GenerateSingleUseAuthToken(string resourceName, IPrincipal user)
		{
			var token = new OneTimeToken
			{
				ResourceName = string.IsNullOrEmpty(resourceName)?"<system>" : resourceName,
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