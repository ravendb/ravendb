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
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, LicenseManager.GetLicenseStatus().ToJson());
            }

            HttpContext.Response.StatusCode = 200;

            return Task.CompletedTask;
        }

        [RavenAction("/license/registration", "POST")]
        public async Task Register()
        {
            UserRegistrationInfo userInfo;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license registration form");
                userInfo = JsonDeserializationServer.UserRegistrationInfo(json);
            }

            await LicenseManager.RegisterForFreeLicense(userInfo).ConfigureAwait(false);

            HttpContext.Response.StatusCode = 200;
        }

        [RavenAction("/license/activate", "POST")]
        public Task Activate()
        {
            License license;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license activation");
                license = JsonDeserializationServer.License(json);
            }

            LicenseManager.Activate(license);
            ServerStore.LicenseStorage.SaveLicense(license);
            HttpContext.Response.StatusCode = 200;

            return Task.CompletedTask;
        }
    }
}