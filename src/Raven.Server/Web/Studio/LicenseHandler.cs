using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio
{
    public class LicenseHandler : RequestHandler
    {
        [RavenAction("/license/status", "GET")]
        public Task Status()
        {
            HttpContext.Response.ContentType = "application/json";
            HttpContext.Response.StatusCode = 200;
            return HttpContext.Response.WriteAsync(JsonConvert.SerializeObject(Commercial.LicenseHandler.GetLicenseStatus()));
        }

        [RavenAction("/license/registration", "POST")]
        public async Task Register()
        {
            var name = GetStringQueryString("name");
            var email = GetStringQueryString("email");
            var company = GetStringQueryString("company", false);
            await Commercial.LicenseHandler.Register(new RegisteredUserInfo
            {
                Name = name,
                Email = email,
                Company = company
            }).ConfigureAwait(false);

            HttpContext.Response.StatusCode = 200;
        }

        [RavenAction("/license/activate", "POST")]
        public Task Activate()
        {
            License license = null;
            var serializer = new JsonSerializer();
            using (var sr = new StreamReader(RequestBodyStream()))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                license = serializer.Deserialize<License>(jsonTextReader);
                if (license == null)
                    throw new InvalidDataException("License cannot be null!");
            }

            Commercial.LicenseHandler.Activate(license);
            ServerStore.LicenseStorage.SaveLicense(license.ToJson());
            HttpContext.Response.StatusCode = 200;
            return Task.CompletedTask;
        }
    }
}