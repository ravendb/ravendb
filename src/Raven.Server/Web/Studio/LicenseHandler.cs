using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Commercial;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class LicenseHandler : RequestHandler
    {
        [RavenAction("/license/eula", "GET", AuthorizationStatus.ValidUser)]
        public async Task Eula()
        {
            HttpContext.Response.ContentType = "text/plain; charset=utf-8";

            using (var stream = typeof(LicenseManager).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Commercial.RavenDB.license.txt"))
            {
                using (var responseStream = ResponseBodyStream())
                {
                    await stream.CopyToAsync(responseStream);
                }
            }
        }

        [RavenAction("/admin/license/eula/accept", "POST", AuthorizationStatus.Operator)]
        public Task AcceptEula()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted)
            {
                NoContent();
                return Task.CompletedTask;
            }

            ServerStore.LicenseManager.AcceptEula();
            return NoContent();
        }

        [RavenAction("/license/status", "GET", AuthorizationStatus.ValidUser)]
        public Task Status()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, ServerStore.LicenseManager.LicenseStatus.ToJson());
            }

            return Task.CompletedTask;
        }
        
        [RavenAction("/license/configuration", "GET", AuthorizationStatus.ValidUser)]
        public Task GetLicenseConfigurationSettings()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var canConfigLicense = true;
                var authentication = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
                if (authentication != null && authentication.Status != RavenServer.AuthenticationStatus.ClusterAdmin)
                {
                    canConfigLicense = false;
                }
                
                var djv = new DynamicJsonValue
                {
                    [nameof(LicenseConfiguration.CanRenew)] = ServerStore.Configuration.Licensing.CanRenew && canConfigLicense,
                    [nameof(LicenseConfiguration.CanActivate)] = ServerStore.Configuration.Licensing.CanActivate && canConfigLicense,
                    [nameof(LicenseConfiguration.CanForceUpdate)] = ServerStore.Configuration.Licensing.CanForceUpdate && canConfigLicense
                };
                
                context.Write(writer, djv);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/license/activate", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task Activate()
        {
            if (ServerStore.Configuration.Licensing.CanActivate == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            License license;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = context.Read(RequestBodyStream(), "license activation");
                license = JsonDeserializationServer.License(json);
            }

            ServerStore.EnsureNotPassive(skipLicenseActivation: true);
            await ServerStore.LicenseManager.Activate(license, GetRaftRequestIdFromQuery());

            NoContentStatus();
        }

        [RavenAction("/admin/license/forceUpdate", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task ForceUpdate()
        {
            if (ServerStore.Configuration.Licensing.CanForceUpdate == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            await ServerStore.LicenseManager.LeaseLicense(GetRaftRequestIdFromQuery(), throwOnError: true);

            NoContentStatus();
        }

        [RavenAction("/license/support", "GET", AuthorizationStatus.ValidUser)]
        public async Task LicenseSupport()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var licenseSupport = await ServerStore.LicenseManager.GetLicenseSupportInfo();
                context.Write(writer, licenseSupport.ToJson());
            }
        }

        [RavenAction("/admin/license/renew", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task RenewLicense()
        {
            if (ServerStore.Configuration.Licensing.CanRenew == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var renewLicense = await ServerStore.LicenseManager.RenewLicense();
                context.Write(writer, renewLicense.ToJson());
            }
        }
    }
}
