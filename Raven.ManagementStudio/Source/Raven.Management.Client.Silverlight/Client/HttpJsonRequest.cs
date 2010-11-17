namespace Raven.Management.Client.Silverlight.Client
{
    using System;
    using System.Net;
    using Abstractions.Data;
    using Common;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A representation of an HTTP json request to the RavenDB server
    /// </summary>
    public class HttpJsonRequest
    {
        private readonly WebRequest webRequest;

        private HttpJsonRequest(Uri url, RequestMethod method, ICredentials credentials)
            : this(url, method, new JObject(), credentials)
        {
        }

        private HttpJsonRequest(Uri url, RequestMethod method, JObject metadata, ICredentials credentials)
        {
            webRequest = WebRequest.Create(url) as HttpWebRequest;
            webRequest.Credentials = credentials;
            WriteMetadata(metadata);
            webRequest.Method = method.GetName();

            switch (method)
            {
                case RequestMethod.POST:
                    webRequest.ContentType = "application/json; charset=utf-8";
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public HttpWebRequest HttpWebRequest
        {
            get { return webRequest as HttpWebRequest; }
        }

        /// <summary>
        /// Gets or sets the response headers.
        /// </summary>
        /// <value>The response headers.</value>
        public NameValueCollection ResponseHeaders { get; set; }

        /// <summary>
        /// Occurs when a json request is created
        /// </summary>
        public static event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate { };

        /// <summary>
        /// Creates the HTTP json request.
        /// </summary>
        /// <param name="self">The self.</param>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="credentials">The credentials.</param>
        /// <returns></returns>
        public static HttpJsonRequest CreateHttpJsonRequest(object self, Uri url, RequestMethod method,
                                                            ICredentials credentials)
        {
            var request = new HttpJsonRequest(url, method, credentials);
            ConfigureRequest(self, new WebRequestEventArgs {Request = request.webRequest});
            return request;
        }

        /// <summary>
        /// Creates the HTTP json request.
        /// </summary>
        /// <param name="self">The self.</param>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="metadata">The metadata.</param>
        /// <param name="credentials">The credentials.</param>
        /// <returns></returns>
        public static HttpJsonRequest CreateHttpJsonRequest(object self, Uri url, RequestMethod method, JObject metadata,
                                                            ICredentials credentials)
        {
            var request = new HttpJsonRequest(url, method, metadata, credentials);
            ConfigureRequest(self, new WebRequestEventArgs {Request = request.webRequest});
            return request;
        }

        private void WriteMetadata(JObject metadata)
        {
            if (metadata == null || metadata.Count == 0)
            {
                webRequest.ContentLength = 0;
                return;
            }

            foreach (var prop in metadata)
            {
                if (prop.Value == null)
                    continue;

                if (prop.Value.Type == JTokenType.Object ||
                    prop.Value.Type == JTokenType.Array)
                    continue;

                string headerName = prop.Key;
                if (headerName == "ETag")
                    headerName = "If-Match";
                string value = prop.Value.Value<object>().ToString();
                switch (headerName)
                {
                    case "Content-Length":
                        break;
                    case "Content-Type":
                        webRequest.ContentType = value;
                        break;
                    default:
                        webRequest.Headers[headerName] = value;
                        break;
                }
            }
        }
    }
}