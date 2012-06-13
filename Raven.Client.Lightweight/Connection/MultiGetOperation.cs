using System;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;

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
				jsonRequestFactory.InvokeLogRequest(holdProfilingInformation, () => new RequestResultArgs
				{
					DurationMilliseconds = httpJsonRequest.CalculateDuration(),
					Method = httpJsonRequest.webRequest.Method,
					HttpResult = 0,
					Status = RequestStatus.AggresivelyCached,
					Result = "",
					Url = httpJsonRequest.webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});
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

					requestStatuses[i] = responses[i] == null ? RequestStatus.AggresivelyCached : RequestStatus.Cached;
					responses[i] = responses[i] ?? new GetResponse { Status = 0 };

					foreach (string header in cachedData[i].Headers)
					{
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

			if (hasCachedRequests == false || convention.DisableProfiling)
				return responses;

			var lastRequest = holdProfilingInformation.ProfilingInformation.Requests.Last();
			for (int i = 0; i < requestStatuses.Length; i++)
			{
				lastRequest.AdditionalInformation["NestedRequestStatus-" + i] = requestStatuses[i].ToString();
			}
			lastRequest.Result = JsonConvert.SerializeObject(responses);

			return responses;
		}
	}
}