using System;
using System.Net;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;

namespace SlowTests.Tests.Faceted
{
    public static class ConditionalGetHelper
    {
        public static HttpStatusCode PerformGet(DocumentStore store, string url, long? requestEtag, out long? responseEtag)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions));

            if (requestEtag != null)
                request.AddHeader("If-None-Match", requestEtag.ToString());

            try
            {
                AsyncHelpers.RunSync(() => request.ReadResponseJsonAsync());
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.NotModified)
                    throw;
            }

            try
            {
                responseEtag = request.ResponseHeaders.GetEtagHeader();
            }
            catch (Exception)
            {
                responseEtag = null;
            }

            return request.ResponseStatusCode;
        }

        public static HttpStatusCode PerformPost(DocumentStore store, string url, string payload, long? requestEtag, out long? responseEtag)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethod.Post, store.DatabaseCommands.PrimaryCredentials, store.Conventions));

            if (requestEtag != null)
                request.AddHeader("If-None-Match", requestEtag.ToString());

            try
            {
                AsyncHelpers.RunSync(() => request.WriteAsync(payload));
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.NotModified)
                    throw;
            }

            try
            {
                responseEtag = request.ResponseHeaders.GetEtagHeader();
            }
            catch (Exception)
            {
                responseEtag = null;
            }

            return request.ResponseStatusCode;
        }
    }
}
