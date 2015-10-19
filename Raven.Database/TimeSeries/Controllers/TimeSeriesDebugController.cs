using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.TimeSeries.Controllers
{
	[RoutePrefix("ts/debug")]
	public class TimeSeriesDebugController : BaseDatabaseApiController
	{
		public class TimeSeriesDebugInfo
		{
			public int ReplicationActiveTasksCount { get; set; }

			public IDictionary<string,TimeSeriesDestinationStats> ReplicationDestinationStats { get; set; }

			public TimeSeriesStats Summary { get; set; }

			public DateTime LastWrite { get; set; }

			public Guid ServerId { get; set; }

			public AtomicDictionary<object> ExtensionsState { get; set; }
		}

		[HttpGet]
		[RavenRoute("time-serieses")]
		public HttpResponseMessage GetTimeSeriesInfo()
		{
			var infos = new List<TimeSeriesDebugInfo>();

			TimeSeriesLandlord.ForAllTimeSeries(ts =>
			{
				using (var reader = ts.CreateReader())
				{
					infos.Add(new TimeSeriesDebugInfo
					{
						ReplicationActiveTasksCount = ts.ReplicationTask.GetActiveTasksCount(),
						ReplicationDestinationStats = ts.ReplicationTask.DestinationStats,
						LastWrite = ts.LastWrite,
						ServerId = ts.ServerId,
						Summary = ts.CreateStats(reader),
						ExtensionsState = ts.ExtensionsState
					});
				}
			});

			return GetMessageWithObject(infos);
		}

		[HttpGet]
		[RavenRoute("ts/{timeSeriesName}/debug/metrics")]
		public async Task<HttpResponseMessage> GetTimeSeriesMetrics(string timeSeriesName)
		{
			var timeSeries = await TimeSeriesLandlord.GetResourceInternal(timeSeriesName).ConfigureAwait(false);
			if (timeSeries == null)
				return GetMessageWithString(string.Format("Time series with name {0} not found.", timeSeriesName), HttpStatusCode.NotFound);

			return GetMessageWithObject(timeSeries.CreateMetrics());
		}
	}
}