using System.Net.Http;
using System.Web.Http;
using Raven.Database.Commercial;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class LicensingController : RavenApiController
	{
		[HttpGet][Route("license/status")]
		public HttpResponseMessage LicenseStatusGet()
		{
			return GetMessageWithObject(ValidateLicense.CurrentLicense);
		}
	}
}