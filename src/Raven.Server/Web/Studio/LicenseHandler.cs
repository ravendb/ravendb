using System.Net;
using System.Threading.Tasks;
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

            await using (var stream = typeof(LicenseManager).Assembly.GetManifestResourceStream("Raven.Server.Commercial.RavenDB.license.txt"))
            {
                await using (var responseStream = ResponseBodyStream())
                {
                    await stream.CopyToAsync(responseStream);
                }
            }
        }

        [RavenAction("/admin/license/eula/accept", "POST", AuthorizationStatus.Operator)]
        public async Task AcceptEula()
        {
            if (ServerStore.LicenseManager.IsEulaAccepted == false)
            {
                await ServerStore.LicenseManager.AcceptEulaAsync();
            }

            NoContentStatus();
        }

        [RavenAction("/license/status", "GET", AuthorizationStatus.ValidUser)]
        public async Task Status()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, ServerStore.LicenseManager.LicenseStatus.ToJson());
            }
        }

        [RavenAction("/license/configuration", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetLicenseConfigurationSettings()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(LicenseConfiguration.CanRenew)] = ServerStore.Configuration.Licensing.CanRenew,
                    [nameof(LicenseConfiguration.CanActivate)] = ServerStore.Configuration.Licensing.CanActivate,
                    [nameof(LicenseConfiguration.CanForceUpdate)] = ServerStore.Configuration.Licensing.CanForceUpdate
                };

                context.Write(writer, djv);
            }
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
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "license activation");
                license = JsonDeserializationServer.License(json);
            }

            await ServerStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
            await ServerStore.LicenseManager.ActivateAsync(license, GetRaftRequestIdFromQuery());

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
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var renewLicense = await ServerStore.LicenseManager.RenewLicense();
                context.Write(writer, renewLicense.ToJson());
            }
        }
    }
}
