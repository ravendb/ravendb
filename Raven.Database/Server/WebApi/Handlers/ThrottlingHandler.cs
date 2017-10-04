// -----------------------------------------------------------------------
//  <copyright file="ThrottlingHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Server.WebApi.Handlers
{
    public class ThrottlingHandler : DelegatingHandler
    {
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        private readonly SemaphoreSlim concurrentRequestSemaphore;

        public ThrottlingHandler(int maxConcurrentServerRequests)
        {
            concurrentRequestSemaphore = new SemaphoreSlim(maxConcurrentServerRequests);
        }

        private const string _debugStr = "/debug/";

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            bool acuiredSemaphore = false;
            try
            {
                TaskCanceledException tce = null;
                // Allow debug endpoints to bypass the concurrent request limit
                if (request.RequestUri.AbsolutePath.Contains(_debugStr) == false)
                {
                    try
                    {
                        acuiredSemaphore = await concurrentRequestSemaphore.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException e)
                    {
                        tce = e;
                    }

                    if (tce != null)
                    {
                        Logger.InfoException("Got task canceled exception.", tce);
                        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false); ;
                    }

                    if (acuiredSemaphore == false)
                    {
                        try
                        {
                            Logger.Info("Too many concurrent requests, throttling! ({0})", request.RequestUri);
                            return await HandleTooBusyError(request).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Logger.WarnException("Could not send a too busy error to the client", e);
                        }
                    }
                }
                return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (acuiredSemaphore)
                    concurrentRequestSemaphore.Release();
            }
        }

        private static Task<HttpResponseMessage> HandleTooBusyError(HttpRequestMessage request)
        {
            var tsc = new TaskCompletionSource<HttpResponseMessage>();
            var response = request.CreateResponse(HttpStatusCode.ServiceUnavailable);
            response.ReasonPhrase = "Service Unavailable";
            response.Content = new JsonContent(RavenJObject.FromObject(new
            {
                Url = request.RequestUri.PathAndQuery,
                Error = "The server is too busy, could not acquire transactional access"
            }));
            tsc.SetResult(response);
            return tsc.Task;
        }
    }
}
