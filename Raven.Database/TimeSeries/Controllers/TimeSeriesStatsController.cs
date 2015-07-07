using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.TimeSeries.Controllers
{
	public class TimeSeriesStatsController : RavenTimeSeriesApiController
    {
		[HttpGet]
		[RavenRoute("ts/{timeSeriesName}/stats")]
		public HttpResponseMessage TimeSeriesStats()
		{
			return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateStats());
		}
    }
}