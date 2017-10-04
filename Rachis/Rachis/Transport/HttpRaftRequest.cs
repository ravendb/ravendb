//-----------------------------------------------------------------------
// <copyright file="HttpRaftRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;

namespace Rachis.Transport
{
    /// <summary>
    /// A representation of an HTTP request to the Raft server
    /// </summary>
    public class HttpRaftRequest : IDisposable
    {
        internal readonly string Url;
        internal readonly HttpMethod HttpMethod;

        private readonly Func<NodeConnectionInfo, Tuple<IDisposable, HttpClient>> _getConnection;
        private readonly CancellationToken _cancellationToken;
        private IDisposable _returnToQueue;
        private readonly NodeConnectionInfo _nodeConnection;
        private bool _isRequestSentToServer;
        private ILog _log;

        public Func<HttpResponseMessage, NodeConnectionInfo, Task<Action<HttpClient>>> UnauthorizedResponseAsyncHandler { get; set; }

        public Func<HttpResponseMessage, NodeConnectionInfo, Task<Action<HttpClient>>> ForbiddenResponseAsyncHandler { get; set; }

        public HttpClient HttpClient { get; private set; }

        public HttpRaftRequest(NodeConnectionInfo nodeConnection, string url, HttpMethod httpMethod, Func<NodeConnectionInfo, Tuple<IDisposable, HttpClient>> getConnection, CancellationToken cancellationToken,ILog logger)
        {
            _log = logger;
            Url = url;
            HttpMethod = httpMethod;
            _getConnection = getConnection;
            _cancellationToken = cancellationToken;
            _nodeConnection = nodeConnection;

            var connection = getConnection(nodeConnection);
            _returnToQueue = connection.Item1;
            HttpClient = connection.Item2;
        }

        private Task SendRequestInternal(Func<HttpRequestMessage> getRequestMessage)
        {
            if (_isRequestSentToServer && Debugger.IsAttached == false)
                throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");
            _isRequestSentToServer = true;

            return RunWithAuthRetry(async () =>
            {
                var requestMessage = getRequestMessage();
                try
                {
                    Response = await HttpClient.SendAsync(requestMessage, _cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    var msg = $"Error while sending a request {requestMessage.Method} {requestMessage.RequestUri}\r\n content:{await GetContentAsString(requestMessage.Content).ConfigureAwait(false)}\r\n{requestMessage.Headers}";
                    _log.ErrorException(msg, e);
                    throw;
                }
                return CheckForAuthErrors();
            });
        }

        /// <summary>
        /// This method gets the string representation of the content of the raft request it is mostly intended for debug and doesn't really show the actual stream content but a logical equivalent.       
        /// </summary>
        /// <param name="requestMessageContent">The content to stringify</param>
        /// <returns></returns>
        private async Task<string> GetContentAsString(HttpContent requestMessageContent)
        {
            var ec = requestMessageContent as HttpTransportSender.EntriesContent;
            if (ec != null)
                return ec.ToString();
            var sc = requestMessageContent as HttpTransportSender.SnapshotContent;
            if (sc != null)
                return sc.ToString();

            var jsonContent = requestMessageContent as JsonContent;
            if (jsonContent != null)
                return jsonContent.Data.ToString();

            var readAsStringAsync = requestMessageContent?.ReadAsStringAsync();
            if (readAsStringAsync != null)
                return await readAsStringAsync.ConfigureAwait(false);

            return string.Empty;
        }

        private async Task RunWithAuthRetry(Func<Task<Boolean>> requestOperation)
        {
            int retries = 0;
            while (true)
            {
                if (await requestOperation().ConfigureAwait(false))
                    return;

                if (++retries >= 3)
                    return;

                if (Response.StatusCode == HttpStatusCode.Forbidden)
                {
                    await HandleForbiddenResponseAsync(Response).ConfigureAwait(false);
                    return;
                }

                if (await HandleUnauthorizedResponseAsync(Response).ConfigureAwait(false) == false)
                    return;
            }
        }

        private bool CheckForAuthErrors()
        {
            return Response.StatusCode != HttpStatusCode.Unauthorized 
                && Response.StatusCode != HttpStatusCode.Forbidden 
                && Response.StatusCode != HttpStatusCode.PreconditionFailed;
        }

        public async Task<HttpResponseMessage> ExecuteAsync()
        {
            await SendRequestInternal(() => new HttpRequestMessage(HttpMethod, Url)).ConfigureAwait(false);

            return Response;
        }

        public async Task<HttpResponseMessage> WriteAsync(Func<HttpContent> content, Dictionary<string,string> headers = null)
        {
            await SendRequestInternal(() =>
            {
                var message = new HttpRequestMessage(HttpMethod, Url)
                {
                    Content = content()
                };

                if (headers != null)
                {
                    foreach (var kvp in headers)
                    {
                        message.Headers.Add(kvp.Key, kvp.Value);
                    }
                }
                return message;
            }).ConfigureAwait(false);

            return Response;
        }

        public async Task<HttpResponseMessage> WriteAsync(Func<HttpContent> content, string headerKey, string headerValue)
        {
            var message = new HttpRequestMessage(HttpMethod, Url)
            {
                Content = content()
            };

            message.Headers.Add(headerKey, headerValue);

            await SendRequestInternal(() => message).ConfigureAwait(false);

            return Response;
        }

        public async Task<bool> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
        {
            if (UnauthorizedResponseAsyncHandler == null)
                return false;

            var unauthorizedResponseAsync = UnauthorizedResponseAsyncHandler(unauthorizedResponse, _nodeConnection);
            if (unauthorizedResponseAsync == null)
                return false;

            var configureHttpClient = await unauthorizedResponseAsync.ConfigureAwait(false);
            RecreateHttpClient(configureHttpClient); 
            return true;
        }

        private async Task HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
        {
            if (ForbiddenResponseAsyncHandler == null)
                return;

            var forbiddenResponseAsync = ForbiddenResponseAsyncHandler(forbiddenResponse, _nodeConnection);
            if (forbiddenResponseAsync == null)
                return;

            await forbiddenResponseAsync.ConfigureAwait(false);
        }

        private void RecreateHttpClient(Action<HttpClient> configureHttpClient)
        {
            var connection = _getConnection(_nodeConnection);
            var newHttpClient = connection.Item2;
            var newReturnToQueue = connection.Item1;
            configureHttpClient(newHttpClient);

            DisposeInternal();

            HttpClient = newHttpClient;
            _returnToQueue = newReturnToQueue;
            _isRequestSentToServer = false;
        }

        public HttpResponseMessage Response { get; private set; }

        public void Dispose()
        {
            DisposeInternal();
        }

        private void DisposeInternal()
        {
            if (Response != null)
            {
                Response.Dispose();
                Response = null;
            }

            if (HttpClient != null)
            {
                if (_returnToQueue != null)
                    _returnToQueue.Dispose();	

                HttpClient = null;
            }
        }
    }
}
