// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;

namespace Raven.Database.Embedded
{
    /// <summary>
    /// This adapts HttpRequestMessages to OWIN requests, dispatches them through the OWIN pipeline, and returns the
    /// associated HttpResponseMessage.
    /// </summary>
    public class OwinClientHandler : HttpMessageHandler
    {
        private readonly Func<IDictionary<string, object>, Task> _next;

        /// <summary>
        /// Create a new handler.
        /// </summary>
        /// <param name="next">The OWIN pipeline entry point.</param>
        public OwinClientHandler(Func<IDictionary<string, object>, Task> next)
        {
            if (next == null)
            {
                throw new ArgumentNullException("next");
            }

            _next = next;
        }

        /// <summary>
        /// This adapts HttpRequestMessages to OWIN requests, dispatches them through the OWIN pipeline, and returns the
        /// associated HttpResponseMessage.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            if (request == null)
            {
                throw new ArgumentNullException("request");
            }

            var state = new RequestState(request, cancellationToken);
            HttpContent requestContent = request.Content ?? new StreamContent(Stream.Null);
            Stream body = await requestContent.ReadAsStreamAsync();
            if (body.CanSeek)
            {
                // This body may have been consumed before, rewind it.
                body.Seek(0, SeekOrigin.Begin);
            }
            state.OwinContext.Request.Body = body;
            CancellationTokenRegistration registration = cancellationToken.Register(state.Abort);

            // Async offload, don't let the test code block the caller.
            Task offload = Task.Factory.StartNew(async () =>
            {
                try
                {
                    await _next(state.Environment);
                    state.CompleteResponse();
                }
                catch (Exception ex)
                {
                    state.Abort(ex);
                }
                finally
                {
                    registration.Dispose();
                    state.Dispose();
                }
            });

            return await state.ResponseTask;
        }

        private class RequestState : IDisposable
        {
            private readonly HttpRequestMessage _request;
            private Action _sendingHeaders;
            private TaskCompletionSource<HttpResponseMessage> _responseTcs;
            private ResponseStream _responseStream;

            internal RequestState(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                _request = request;
                _responseTcs = new TaskCompletionSource<HttpResponseMessage>();
                _sendingHeaders = () => { };

                if (request.RequestUri.IsDefaultPort)
                {
                    request.Headers.Host = request.RequestUri.Host;
                }
                else
                {
                    request.Headers.Host = request.RequestUri.GetComponents(UriComponents.HostAndPort, UriFormat.UriEscaped);
                }

                OwinContext = new OwinContext();
                OwinContext.Set("owin.Version", "1.0");
                IOwinRequest owinRequest = OwinContext.Request;
                owinRequest.Protocol = "HTTP/" + request.Version.ToString(2);
                owinRequest.Scheme = request.RequestUri.Scheme;
                owinRequest.Method = request.Method.ToString();
                owinRequest.Path = PathString.FromUriComponent(request.RequestUri);
                owinRequest.PathBase = PathString.Empty;
                owinRequest.QueryString = QueryString.FromUriComponent(request.RequestUri);
                owinRequest.CallCancelled = cancellationToken;
                owinRequest.Set<Action<Action<object>, object>>("server.OnSendingHeaders", (callback, state) =>
                {
                    var prior = _sendingHeaders;
                    _sendingHeaders = () =>
                    {
                        prior();
                        callback(state);
                    };
                });

                foreach (var header in request.Headers)
                {
                    owinRequest.Headers.AppendValues(header.Key, header.Value.ToArray());
                }
                HttpContent requestContent = request.Content;
                if (requestContent != null)
                {
                    foreach (var header in request.Content.Headers)
                    {
                        owinRequest.Headers.AppendValues(header.Key, header.Value.ToArray());
                    }
                }

                _responseStream = new ResponseStream(CompleteResponse);
                OwinContext.Response.Body = _responseStream;
                OwinContext.Response.StatusCode = 200;
            }

            public IOwinContext OwinContext { get; private set; }

            public IDictionary<string, object> Environment
            {
                get { return OwinContext.Environment; }
            }

            public Task<HttpResponseMessage> ResponseTask
            {
                get { return _responseTcs.Task; }
            }

            internal void CompleteResponse()
            {
                if (!_responseTcs.Task.IsCompleted)
                {
                    HttpResponseMessage response = GenerateResponse();
                    // Dispatch, as TrySetResult will synchronously execute the waiters callback and block our Write.
                    Task.Factory.StartNew(() => _responseTcs.TrySetResult(response));
                }
            }

            internal HttpResponseMessage GenerateResponse()
            {
                _sendingHeaders();

                var response = new HttpResponseMessage();
                response.StatusCode = (HttpStatusCode)OwinContext.Response.StatusCode;
                response.ReasonPhrase = OwinContext.Response.ReasonPhrase;
                response.RequestMessage = _request;
                // response.Version = owinResponse.Protocol;

                response.Content = new StreamContent(_responseStream);

                foreach (var header in OwinContext.Response.Headers)
                {
                    if (!response.Headers.TryAddWithoutValidation(header.Key, header.Value))
                    {
                        bool success = response.Content.Headers.TryAddWithoutValidation(header.Key, header.Value);
                        Contract.Assert(success, "Bad header");
                    }
                }
                return response;
            }

            internal void Abort()
            {
                Abort(new OperationCanceledException());
            }

            internal void Abort(Exception exception)
            {
                _responseStream.Abort(exception);
                _responseTcs.TrySetException(exception);
            }

            public void Dispose()
            {
                _responseStream.Dispose();
                // Do not dispose the request, that will be disposed by the caller.
            }
        }
    }
}