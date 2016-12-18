using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Connection.Implementation;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    internal class MultiGetOperation
    {
        private readonly IHoldProfilingInformation holdProfilingInformation;
        private readonly DocumentConvention convention;
        private readonly string url;
        private readonly GetRequest[] requests;
        private readonly string requestUri;
        private bool allRequestsCanBeServedFromAggressiveCache;
        private CachedRequest[] cachedData;

        public string RequestUri
        {
            get { return requestUri; }
        }

        public MultiGetOperation(
            IHoldProfilingInformation holdProfilingInformation,
            DocumentConvention convention,
            string url,
            GetRequest[] requests)
        {
            this.holdProfilingInformation = holdProfilingInformation;
            this.convention = convention;
            this.url = url;
            this.requests = requests;

            requestUri = url + "/multi_get";
            if (convention.UseParallelMultiGet)
            {
                requestUri += "?parallel=yes";
            }
        }

        public GetRequest[] PreparingForCachingRequest(HttpJsonRequestFactory jsonRequestFactory)
        {
            cachedData = new CachedRequest[requests.Length];
            var requestsForServer = new GetRequest[requests.Length];
            Array.Copy(requests, 0, requestsForServer, 0, requests.Length);
            if (jsonRequestFactory.DisableHttpCaching == false && convention.ShouldCacheRequest(requestUri))
            {
                for (int i = 0; i < requests.Length; i++)
                {
                    var request = requests[i];
                    var cachingConfiguration = jsonRequestFactory.ConfigureCaching(url + request.UrlAndQuery,
                                                                                   (key, val) => request.Headers[key] = val);
                    cachedData[i] = cachingConfiguration.CachedRequest;
                    if (cachingConfiguration.SkipServerCheck)
                        requestsForServer[i] = null;
                }
            }
            allRequestsCanBeServedFromAggressiveCache = requestsForServer.All(x => x == null);
            return requestsForServer;
        }

        public bool CanFullyCache(HttpJsonRequestFactory jsonRequestFactory, HttpJsonRequest httpJsonRequest, string postedData)
        {
            if (allRequestsCanBeServedFromAggressiveCache) // can be fully served from aggressive cache
            {
                if (jsonRequestFactory.CanLogRequest)
                {

                    jsonRequestFactory.OnLogRequest(holdProfilingInformation, new RequestResultArgs
                    {
                        DurationMilliseconds = httpJsonRequest.CalculateDuration(),
                        Method = httpJsonRequest.Method,
                        HttpResult = 0,
                        Status = RequestStatus.AggressivelyCached,
                        Result = "",
                        Url = httpJsonRequest.Url.ToString(),
                        //TODO: check that is the same as: Url = httpJsonRequest.webRequest.RequestUri.PathAndQuery,
                        PostedData = postedData
                    });
                }
                return true;
            }
            return false;
        }

        public GetResponse[] HandleCachingResponse(GetResponse[] responses, HttpJsonRequestFactory jsonRequestFactory)
        {
            var hasCachedRequests = false;
            var requestStatuses = new RequestStatus[responses.Length];
            for (int i = 0; i < responses.Length; i++)
            {
                if (responses[i] == null || responses[i].Status == 304)
                {
                    hasCachedRequests = true;

                    requestStatuses[i] = responses[i] == null ? RequestStatus.AggressivelyCached : RequestStatus.Cached;
                    responses[i] = responses[i] ?? new GetResponse { Status = 0 };

                    foreach (string header in cachedData[i].Headers)
                    {
                        cachedData[i].Time = SystemTime.UtcNow;
                        responses[i].Headers[header] = cachedData[i].Headers[header];
                    }
                    responses[i].Result = cachedData[i].Data.CloneToken();
                    jsonRequestFactory.IncrementCachedRequests();
                }
                else
                {
                    requestStatuses[i] = responses[i].RequestHasErrors() ? RequestStatus.ErrorOnServer : RequestStatus.SentToServer;

                    var nameValueCollection = new NameValueCollection();
                    foreach (var header in responses[i].Headers)
                    {
                        nameValueCollection[header.Key] = header.Value;
                    }
                    jsonRequestFactory.CacheResponse(url + requests[i].UrlAndQuery, responses[i].Result, nameValueCollection);
                }
            }

            if (hasCachedRequests == false || convention.DisableProfiling ||
                holdProfilingInformation.ProfilingInformation.Requests.Count == 0)
                return responses;

            var lastRequest = holdProfilingInformation.ProfilingInformation.Requests.Last();
            for (int i = 0; i < requestStatuses.Length; i++)
            {
                lastRequest.AdditionalInformation["NestedRequestStatus-" + i] = requestStatuses[i].ToString();
            }
            lastRequest.Result = JsonConvert.SerializeObject(responses);

            return responses;
        }

        public async Task TryResolveConflictOrCreateConcurrencyException(GetResponse[] responses, Func<string, RavenJObject, Etag, Task<ConflictException>> tryResolveConflictOrCreateConcurrencyException)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            // see: http://ayende.com/blog/169377/the-bug-that-ruined-my-weekend
            for (int index = 0; index < responses.Length; index++)
            {
                var response = responses[index];
                if (response == null)
                    continue;
                if (response.RequestHasErrors() && response.Status != 409)
                    continue;

                var result = response.Result as RavenJObject;
                if (result == null)
                    continue;

                if (result.ContainsKey("Results"))
                {
                    var results = result["Results"] as RavenJArray;
                    if (results == null)
                        continue;

                    foreach (RavenJToken value in results)
                    {
                        var docResult = value as RavenJObject;
                        if (docResult == null)
                            return;

                        var metadata = docResult[Constants.Metadata];
                        if (metadata == null)
                            return;

                        if (metadata.Value<int>("@Http-Status-Code") != 409)
                            return;

                        var id = metadata.Value<string>("@id");
                        var etag = HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag"));

                        await TryResolveConflictOrCreateConcurrencyExceptionForSingleDocument(
                            tryResolveConflictOrCreateConcurrencyException,
                            id,
                            etag,
                            docResult,
                            response)
                            .ConfigureAwait(false);
                    }

                    continue;
                }
                if (result.ContainsKey("Conflicts"))
                {
                    var id = response.Headers[Constants.DocumentIdFieldName];
                    var etag = response.GetEtagHeader();

                    await TryResolveConflictOrCreateConcurrencyExceptionForSingleDocument(
                        tryResolveConflictOrCreateConcurrencyException,
                        id,
                        etag,
                        result,
                        response)
                        .ConfigureAwait(false);
                }
            }
        }

        private static async Task TryResolveConflictOrCreateConcurrencyExceptionForSingleDocument(
            Func<string, RavenJObject, Etag, Task<ConflictException>> tryResolveConflictOrCreateConcurrencyException,
            string id,
            Etag etag,
            RavenJObject docResult,
            GetResponse response)
        {
            var concurrencyException = await tryResolveConflictOrCreateConcurrencyException(id, docResult, etag).ConfigureAwait(false);

            if (concurrencyException != null)
                throw concurrencyException;

            response.Status = 200;
            response.ForceRetry = true;
        }
    }
}
