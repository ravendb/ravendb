using System;
using System.Net;
using Raven.Database.Config;

namespace Raven.Database.Server.Security.OAuth
{

	public class OAuthConfigureHttpListener : IConfigureHttpListener
	{
		public void Configure(HttpListener listener, InMemoryRavenConfiguration config)
		{
			if (string.Equals(config.AuthenticationMode, "OAuth", StringComparison.InvariantCultureIgnoreCase) == false) 
				return;

			listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous | AuthenticationSchemes.Basic;

			listener.AuthenticationSchemeSelectorDelegate = request => {
				return request.RawUrl.StartsWith("/OAuth/AccessToken", StringComparison.InvariantCultureIgnoreCase) ?
					AuthenticationSchemes.Basic | AuthenticationSchemes.Anonymous : AuthenticationSchemes.Anonymous;
			};
		}
	}
}