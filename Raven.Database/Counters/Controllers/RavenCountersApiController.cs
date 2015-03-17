// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public abstract class RavenCountersApiController : RavenBaseApiController
	{
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		private PagingInfo paging;
		private NameValueCollection queryString;

		private CountersLandlord landlord;
		private RequestManager requestManager;
        
		public RequestManager RequestManager
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
                var counterStorage = CountersLandlord.GetCounterInternal(CountersName);
                if (counterStorage == null)
                {
                    throw new InvalidOperationException("Could not find a counter storage named: " + CountersName);
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
					RequestManager.SetThreadLocalState(ReadInnerHeaders, CountersName);
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

			var fileSystemInternal = await CountersLandlord.GetCounterInternal(CountersName);
			if (fileSystemInternal == null)
			{
				var msg = "Could not find a counters named: " + CountersName;
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
				var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("counterName"));

				if (selectedData != null)
					CountersName = selectedData.Values["counterName"] as string;
			}
			else
			{
				if (values.ContainsKey("cou"))
					CountersName = values["counterName"] as string;
			}
			if (CountersName == null)
				throw new InvalidOperationException("Could not find counter name for this request");
		}

		public string CountersName { get; private set; }

		public CountersLandlord CountersLandlord
		{
			get
			{
				if (Configuration == null)
					return landlord;
				return (CountersLandlord)Configuration.Properties[typeof(CountersLandlord)];
			}
		}


	

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
	        get { throw new NotImplementedException(); }
	    }

	    public override async Task<bool> SetupRequestToProperDatabase(RequestManager rm)
		{
			var tenantId = CountersName;

			if (string.IsNullOrWhiteSpace(tenantId))
			{
				throw new HttpException(503, "Could not find a counter with no name");
			}

			Task<CounterStorage> resourceStoreTask;
			bool hasDb;
			try
			{
				hasDb = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
			}
			catch (Exception e)
			{
				var msg = "Could open counter named: " + tenantId;
				Logger.WarnException(msg, e);
				throw new HttpException(503, msg, e);
			}
			if (hasDb)
			{
				try
				{
                    if (await Task.WhenAny(resourceStoreTask, Task.Delay(TimeSpan.FromSeconds(30))) != resourceStoreTask)
					{
						var msg = "The counter " + tenantId +
								  " is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, file system loading continues.";
						Logger.Warn(msg);
						throw new HttpException(503, msg);
					}
					var args = new BeforeRequestWebApiEventArgs
					{
						Controller = this,
						IgnoreRequest = false,
						TenantId = tenantId,
						Counters = resourceStoreTask.Result
					};
					rm.OnBeforeRequest(args);
					if (args.IgnoreRequest)
						return false;
				}
				catch (Exception e)
				{
					var msg = "Could open counters named: " + tenantId;
					Logger.WarnException(msg, e);
					throw new HttpException(503, msg, e);
				}

				landlord.LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
			}
			else
			{
				var msg = "Could not find a counter named: " + tenantId;
				Logger.Warn(msg);
				throw new HttpException(503, msg);
			}
			return true;
		}

		public override string TenantName
		{
			get { return "counters/" + CountersName; }
		}

        public override void MarkRequestDuration(long duration)
        {
            if (Storage == null)
                return;
            Storage.MetricsCounters.RequestDuationMetric.Update(duration);
        }

		public override InMemoryRavenConfiguration SystemConfiguration
		{
			get { return CountersLandlord.SystemConfiguration; }
		}

       

		public CounterStorage Storage
		{
			get
			{
				var counter = CountersLandlord.GetCounterInternal(CountersName);
				if (counter == null)
				{
					throw new InvalidOperationException("Could not find a counter named: " + CountersName);
				}

				return counter.Result;
			}
		}

	
	}
}