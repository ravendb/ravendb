using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.TimeSeries;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.TimeSeries.Controllers
{
	public class StatsController : RavenTimeSeriesApiController
    {
		[RavenRoute("ts/{timeSeriesName}/stats")]
		[HttpGet]
		public HttpResponseMessage TimeSeriesStats()
		{
			return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateStats());
		}
    }
}