using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	public class StatisticsController : RavenDbApiController
	{
		[HttpGet]
		[Route("stats")]
		[Route("databases/{databaseName}/stats")]
		public HttpResponseMessage Get()
		{
			return GetMessageWithObject(Database.Statistics);
		}
	}
}