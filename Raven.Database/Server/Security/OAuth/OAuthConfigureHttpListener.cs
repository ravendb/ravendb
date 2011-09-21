using System;
using System.Net;

namespace Raven.Http.Security.OAuth
{

	public class OAuthConfigureHttpListener : IConfigureHttpListener
	{
		public void Configure(HttpListener listener, IRavenHttpConfiguration config)
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