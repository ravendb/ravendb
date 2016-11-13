using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.OAuth;

namespace Raven.NewClient.Client.Extensions
{
    internal static class SecurityExtensions
    {
        //TODO: this can be used in document store/counter stores/time series store
        internal static void InitializeSecurity(ConventionBase conventions, HttpJsonRequestFactory requestFactory, string serverUrl, ICredentials primaryCredentials)
        {
            if (conventions.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

            var securedAuthenticator = new SecuredAuthenticator();

            requestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            conventions.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
            {
                var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
                // Make sure to avoid a cross DNS security issue, when running with Fiddler
                if (string.IsNullOrEmpty(oauthSource) == false)
                    oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

                if (credentials.ApiKey == null)
                {
                    return null;
                }

                if (string.IsNullOrEmpty(oauthSource))
                    oauthSource = serverUrl + "/OAuth/API-Key";

                return securedAuthenticator.DoOAuthRequestAsync(oauthSource, credentials.ApiKey);
            };

        }
    }
}
