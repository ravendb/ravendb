using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Abstractions.Connection;

namespace Raven.Abstractions.OAuth
{
    public abstract class AbstractAuthenticator
    {
        public string CurrentToken { get; set; }
        public string CurrentTokenWithBearer { get; set; }

        public virtual void ConfigureRequest(object sender, WebRequestEventArgs e)
        {
            SetAuthorization(e);
        }

        protected void SetAuthorization(WebRequestEventArgs e)
        {
            if (string.IsNullOrEmpty(CurrentToken))
                return;

            if (e.Client == null)
                return;
            SetAuthorization(e.Client);
        }

        protected void SetAuthorization(HttpClient e)
        {
            if (string.IsNullOrEmpty(CurrentToken))
                return;

            try
            {
                e.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", CurrentToken);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Could not set the Authorization to the value 'Bearer {0}'", CurrentToken), ex);
            }
        }

        protected static void SetHeader(WebHeaderCollection headers, string key, string value)
        {
            try
            {
                headers[key] = value;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not set '" + key + "' = '" + value + "'", e);
            }
        }
    }
}
