using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	public class BuildController : RavenApiController
	{
		[HttpGet]
		public HttpResponseMessage Version()
		{
			return GetMessageWithObject(new
			{
				DocumentDatabase.ProductVersion,
				DocumentDatabase.BuildVersion
			});
		}
	}
}
