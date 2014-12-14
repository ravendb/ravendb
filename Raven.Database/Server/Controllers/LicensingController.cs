using System.Net.Http;
using System.Web.Http;
using Raven.Database.Commercial;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class LicensingController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("license/status")]
		public HttpResponseMessage LicenseStatusGet()
		{
			return GetMessageWithObject(ValidateLicense.CurrentLicense);
		}
	}
}