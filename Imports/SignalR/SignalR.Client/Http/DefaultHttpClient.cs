using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Raven.Imports.SignalR.Client.Http
{
    /// <summary>
    /// The default <see cref="IHttpClient"/> implementation.
    /// </summary>
    public class DefaultHttpClient : IHttpClient
    {
        /// <summary>
        /// Makes an asynchronous http GET request to the specified url.
        /// </summary>
        /// <param name="url">The url to send the request to.</param>
        /// <param name="prepareRequest">A callback that initializes the request with default values.</param>
        /// <returns>A <see cref="Task{IResponse}"/>.</returns>
        public Task<IResponse> GetAsync(string url, Action<IRequest> prepareRequest)
        {
        	HttpWebRequest cachedRequest = null;
            return HttpHelper.GetAsync(url, request => prepareRequest(new HttpWebRequestWrapper(cachedRequest = request)))
							 .Then(response => (IResponse)new HttpWebResponseWrapper(cachedRequest, response));
        }

        /// <summary>
        /// Makes an asynchronous http POST request to the specified url.
        /// </summary>
        /// <param name="url">The url to send the request to.</param>
        /// <param name="prepareRequest">A callback that initializes the request with default values.</param>
        /// <param name="postData">form url encoded data.</param>
        /// <returns>A <see cref="Task{IResponse}"/>.</returns>
        public Task<IResponse> PostAsync(string url, Action<IRequest> prepareRequest, Dictionary<string, string> postData)
        {
        	HttpWebRequest cachedRequest = null;
            return HttpHelper.PostAsync(url, request => prepareRequest(new HttpWebRequestWrapper(cachedRequest = request)), postData)
							 .Then(response => (IResponse)new HttpWebResponseWrapper(cachedRequest, response));
        }
    }
}
