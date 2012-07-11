using System;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Database.Config;
using System.Linq;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsAuthConfigureHttpListener : IConfigureHttpListener
	{
		public static Regex IsAdminRequest = new Regex(@"(^/admin)|(^/databases/[\w.-_\d]+/admin)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

		public void Configure(HttpListener listener, InMemoryRavenConfiguration config)
		{
			if (string.Equals(config.AuthenticationMode, "Windows",StringComparison.InvariantCultureIgnoreCase) == false) 
				return;

			listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
												 AuthenticationSchemes.Anonymous;
			
			switch (config.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.None:
					listener.AuthenticationSchemeSelectorDelegate = request =>
					{
						if (NeverSecret.Urls.Contains(request.Url.AbsolutePath))
							return AuthenticationSchemes.Anonymous;
						return AuthenticationSchemes.IntegratedWindowsAuthentication;
					};
					break;
				case AnonymousUserAccessMode.All:
					listener.AuthenticationSchemeSelectorDelegate = request =>
					{
						if (IsAdminRequest.IsMatch(request.RawUrl))
							return AuthenticationSchemes.IntegratedWindowsAuthentication;

						return AuthenticationSchemes.Anonymous;
					};
					break;
				case AnonymousUserAccessMode.Get:
					listener.AuthenticationSchemeSelectorDelegate = request =>
					{
						if (NeverSecret.Urls.Contains(request.Url.AbsolutePath))
							return AuthenticationSchemes.Anonymous;
					
						return AbstractRequestAuthorizer.IsGetRequest(request.HttpMethod, request.Url.AbsolutePath) ?
							AuthenticationSchemes.Anonymous | AuthenticationSchemes.IntegratedWindowsAuthentication :
							AuthenticationSchemes.IntegratedWindowsAuthentication;
					};
					break;
				default:
					throw new ArgumentException(string.Format("Cannot understand access mode: '{0}'", config.AnonymousUserAccessMode));
			}
		}
	}
}