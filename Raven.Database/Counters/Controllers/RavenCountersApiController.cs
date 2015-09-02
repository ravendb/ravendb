// -----------------------------------------------------------------------
//  <copyright file="RavenCountersApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Counters.Controllers
{
	public abstract class RavenCountersApiController : RavenBaseApiController
	{
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		private PagingInfo paging;
		private NameValueCollection queryString;

		private CountersLandlord landlord;
		private RequestManager requestManager;
        
		new public RequestManager RequestManager
		{
			get
			{
				if (Configuration == null)
					return requestManager;
				return (RequestManager)Configuration.Properties[typeof(RequestManager)];
			}
		}

	    public CounterStorage CounterStorage
	    {
	        get
	        {
		        if (string.IsNullOrWhiteSpace(CounterStorageName))
			        throw new InvalidOperationException("Could not find counter storage name in path.. maybe it is missing or the request URL is malformed?");

		        var counterStorage = CountersLandlord.GetCounterInternal(CounterStorageName);
                if (counterStorage == null)
                {
                    throw new InvalidOperationException("Could not find a counter storage named: " + CounterStorageName);
                }

                return counterStorage.Result;
	        }
	    }

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			var result = new HttpResponseMessage();
			if (InnerRequest.Method.Method != "OPTIONS")
			{
				result = await RequestManager.HandleActualRequest(this, controllerContext, async () =>
				{
					RequestManager.SetThreadLocalState(ReadInnerHeaders, CounterStorageName);
					return await ExecuteActualRequest(controllerContext, cancellationToken, authorizer);
				}, httpException => GetMessageWithObject(new { Error = httpException.Message }, HttpStatusCode.ServiceUnavailable));
			}

			RequestManager.AddAccessControlHeaders(this, result);
			RequestManager.ResetThreadLocalState();

			return result;
		}

		private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken,
			MixedModeRequestAuthorizer authorizer)
		{
			HttpResponseMessage authMsg;
			if (authorizer.TryAuthorize(this, out authMsg) == false)
				return authMsg;

            if (IsInternalRequest == false)
				RequestManager.IncrementRequestCount();

			var counterInternal = await CountersLandlord.GetCounterInternal(CounterStorageName);
			if (counterInternal == null)
			{
				var msg = "Could not find a counters named: " + CounterStorageName;
				return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
			}

			var sp = Stopwatch.StartNew();

			var result = await base.ExecuteAsync(controllerContext, cancellationToken);
			sp.Stop();
			AddRavenHeader(result, sp);

			return result;
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);
			landlord = (CountersLandlord)controllerContext.Configuration.Properties[typeof(CountersLandlord)];
			requestManager = (RequestManager)controllerContext.Configuration.Properties[typeof(RequestManager)];

			var values = controllerContext.Request.GetRouteData().Values;
			if (values.ContainsKey("MS_SubRoutes"))
			{
				var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
				var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("counterStorageName"));

				if (selectedData != null)
					CounterStorageName = selectedData.Values["counterStorageName"] as string;
			}
			else
			{
				if (values.ContainsKey("counterStorageName"))
					CounterStorageName = values["counterStorageName"] as string;
			}
		}

		public string CounterStorageName { get; private set; }

		private NameValueCollection QueryString
		{
			get { return queryString ?? (queryString = HttpUtility.ParseQueryString(Request.RequestUri.Query)); }
		}

		protected PagingInfo Paging
		{
			get
			{
				if (paging != null)
					return paging;

				int start;
				int.TryParse(QueryString["start"], out start);

				int pageSize;
				if (int.TryParse(QueryString["pageSize"], out pageSize) == false)
					pageSize = 25;

				paging = new PagingInfo
				{
					PageSize = Math.Min(1024, Math.Max(1, pageSize)),
					Start = Math.Max(start, 0)
				};

				return paging;
			}
		}

		protected class PagingInfo
		{
			public int PageSize;
			public int Start;
		}

	    public override InMemoryRavenConfiguration ResourceConfiguration
	    {
	        get { return CounterStorage.Configuration; }
	    }

		public override async Task<RequestWebApiEventArgs> TrySetupRequestToProperResource()
		{
			var tenantId = CounterStorageName;
			if (string.IsNullOrWhiteSpace(tenantId))
				throw new HttpException(503, "Could not find a counter storage with no name");

			Task<CounterStorage> resourceStoreTask;
			bool hasCounter;
			string msg;
			try
			{
				hasCounter = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
			}
			catch (Exception e)
			{
				msg = "Could not open counter named: " + tenantId;
				Logger.WarnException(msg, e);
				throw new HttpException(503, msg, e);
			}
			if (hasCounter)
			{
				try
				{
                    if (await Task.WhenAny(resourceStoreTask, Task.Delay(TimeSpan.FromSeconds(30))) != resourceStoreTask)
					{
						msg = "The counter " + tenantId +
								  " is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, counter storage loading continues.";
						Logger.Warn(msg);
						throw new HttpException(503, msg);
					}

					landlord.LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

					return new RequestWebApiEventArgs
					{
						Controller = this,
						IgnoreRequest = false,
						TenantId = tenantId,
						Counters = resourceStoreTask.Result
					};
				}
				catch (Exception e)
				{
					msg = "Could open counters named: " + tenantId;
					Logger.WarnException(msg, e);
					throw new HttpException(503, msg, e);
				}
			}

			msg = "Could not find a counter named: " + tenantId;
			Logger.Warn(msg);
			throw new HttpException(503, msg);
		}

		public override string TenantName
		{
			get { return TenantNamePrefix + CounterStorageName; }
		}

		private const string TenantNamePrefix = "cs/";

        public override void MarkRequestDuration(long duration)
        {
            if (Storage == null)
                return;
            Storage.MetricsCounters.RequestDurationMetric.Update(duration);
        }

		public override InMemoryRavenConfiguration SystemConfiguration
		{
			get { return CountersLandlord.SystemConfiguration; }
		}

		public CounterStorage Storage
		{
			get
			{
				var counter = CountersLandlord.GetCounterInternal(CounterStorageName);
				if (counter == null)
				{
					throw new InvalidOperationException("Could not find a counter storage named: " + CounterStorageName);
				}

				return counter.Result;
			}
		}
	}
}