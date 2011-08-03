using System;
using System.Net;

namespace Raven.Http.Security.Windows
{
    public class WindowsAuthConfigureHttpListener : IConfigureHttpListener
    {
        public void Configure(HttpListener listener, IRavenHttpConfiguration config)
        {
            if (config.AuthenticationMode != "Windows") return;

            switch (config.AnonymousUserAccessMode)
            {
                case AnonymousUserAccessMode.None:
                    listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;
                    break;
                case AnonymousUserAccessMode.All:
                    listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
                       AuthenticationSchemes.Anonymous;
                    listener.AuthenticationSchemeSelectorDelegate = request =>
                    {
                        if (request.RawUrl.StartsWith("/admin", StringComparison.InvariantCultureIgnoreCase))
                            return AuthenticationSchemes.IntegratedWindowsAuthentication;

                        return AuthenticationSchemes.Anonymous;
                    };
                    break;
                case AnonymousUserAccessMode.Get:
                    listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
                        AuthenticationSchemes.Anonymous;
                    listener.AuthenticationSchemeSelectorDelegate = request =>
                    {
                        return AbstractRequestAuthorizer.IsGetRequest(request.HttpMethod, request.Url.AbsolutePath) ?
                            AuthenticationSchemes.Anonymous | AuthenticationSchemes.IntegratedWindowsAuthentication :
                            AuthenticationSchemes.IntegratedWindowsAuthentication;
                    };
                    break;
                default:
                    throw new ArgumentException("Cannot understand access mode: " + config.AnonymousUserAccessMode);
            }
        }
    }
}