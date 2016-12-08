using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class LicenseHandler : RequestHandler
    {
        [RavenAction("/license/status", "GET")]
        public Task Status()
        {
            HttpContext.Response.StatusCode = 200;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, LicenseManager.GetLicenseStatus().ToJson());
            }

            return Task.CompletedTask;
        }

        [RavenAction("/license/registration", "POST")]
        public async Task Register()
        {
            HttpContext.Response.StatusCode = 200;

            UserRegistrationInfo userInfo;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license registration form");
                userInfo = JsonDeserializationServer.UserRegistrationInfo(json);
            }

            await LicenseManager.RegisterForFreeLicense(userInfo).ConfigureAwait(false);
        }

        [RavenAction("/license/activate", "POST")]
        public Task Activate()
        {
            HttpContext.Response.StatusCode = 200;

            License license;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license activation");
                license = JsonDeserializationServer.License(json);
            }

            LicenseManager.Activate(license);

            return Task.CompletedTask;
        }
    }
}