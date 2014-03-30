using System;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Database.Config;
using System.Linq;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsAuthConfigureHttpListener
	{
		public static Regex IsAdminRequest = new Regex(@"(^/admin)|(^/databases/[\w\.\-_]+/admin)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

		private InMemoryRavenConfiguration configuration;
		public void Configure(HttpListener listener, InMemoryRavenConfiguration config)
		{
			configuration = config;
			listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
												 AuthenticationSchemes.Anonymous;
			
			listener.AuthenticationSchemeSelectorDelegate += AuthenticationSchemeSelectorDelegate;
		}

		private AuthenticationSchemes AuthenticationSchemeSelectorDelegate(HttpListenerRequest request)
		{
			var authHeader = request.Headers["Authorization"];
			var hasApiKey = "True".Equals(request.Headers["Has-Api-Key"], StringComparison.CurrentCultureIgnoreCase);
			var hasSingleUseToken = string.IsNullOrEmpty(request.Headers["Single-Use-Auth-Token"]) == false;
			var hasOAuthTokenInCookie = request.Cookies["OAuth-Token"] != null;
			if (hasApiKey || hasOAuthTokenInCookie || hasSingleUseToken  ||
					string.IsNullOrEmpty(authHeader) == false && authHeader.StartsWith("Bearer "))
			{
				// this is an OAuth request that has a token
				// we allow this to go through and we will authenticate that on the OAuth Request Authorizer
				return AuthenticationSchemes.Anonymous;
			}
			if (NeverSecret.IsNeverSecretUrl(request.Url.AbsolutePath))
				return AuthenticationSchemes.Anonymous;

			//CORS pre-flight.
			if(!String.IsNullOrEmpty(configuration.AccessControlAllowOrigin) && request.HttpMethod == "OPTIONS") 
				{ return AuthenticationSchemes.Anonymous; }

			if (IsAdminRequest.IsMatch(request.RawUrl) && 
				configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
				return AuthenticationSchemes.IntegratedWindowsAuthentication;

			switch (configuration.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.Admin:
				case AnonymousUserAccessMode.All:
					return AuthenticationSchemes.Anonymous;
				case AnonymousUserAccessMode.Get:
					return AbstractRequestAuthorizer.IsGetRequest(request.HttpMethod, request.Url.AbsolutePath) ?
						AuthenticationSchemes.Anonymous | AuthenticationSchemes.IntegratedWindowsAuthentication :
						AuthenticationSchemes.IntegratedWindowsAuthentication;
				case AnonymousUserAccessMode.None:
					return AuthenticationSchemes.IntegratedWindowsAuthentication;
				default:
					throw new ArgumentException(string.Format("Cannot understand access mode: '{0}'", configuration.AnonymousUserAccessMode));
			}
		}
	}
}