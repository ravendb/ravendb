using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Commercial;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
    public class LicensingController : BaseDatabaseApiController
    {
        [HttpGet]
        [RavenRoute("license/status")]
        public HttpResponseMessage LicenseStatusGet()
        {
            if (EnsureSystemDatabase() == false)
                return
                    GetMessageWithString(
                        "The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database",
                        HttpStatusCode.BadRequest);

            // This method is NOT secured, and anyone can access it.
            return GetMessageWithObject(ValidateLicense.CurrentLicense);
        }

        [HttpGet]
        [RavenRoute("license/support")]
        public HttpResponseMessage SupportStatus()
        {
            if (EnsureSystemDatabase() == false)
                return
                    GetMessageWithString(
                        "The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database",
                        HttpStatusCode.BadRequest);

            return GetMessageWithObject(SupportCoverage.CurrentSupport);
        }
    }
}
