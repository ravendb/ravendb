using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Server.Commercial;
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
                context.Write(writer, ServerStore.LicenseManager.GetLicenseStatus().ToJson());
            }

            return Task.CompletedTask;
        }
        
        [RavenAction("/license/configuration", "GET", AuthorizationStatus.ValidUser)]
        public Task GetLicenseConfigurationSettings()
        {
            var licenseConfiguration = new LicenseConfigurationSettings
            {
                RenewSettings = ServerStore.Configuration.Licensing.CanRenewLicense,
                ActivateSettings = ServerStore.Configuration.Licensing.CanActivate,
                ForceUpdateSettings = ServerStore.Configuration.Licensing.CanForceUpdate
            };

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, licenseConfiguration.ToJson());
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

            await ServerStore.LicenseManager.Activate(license, skipLeaseLicense: false, GetRaftRequestIdFromQuery());

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

            await ServerStore.LicenseManager.LeaseLicense(GetRaftRequestIdFromQuery(), forceUpdate: true);

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
            if (ServerStore.Configuration.Licensing.CanRenewLicense == false)
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
    
    public class LicenseConfigurationSettings : IDynamicJson
    {
        public bool RenewSettings { get; set; }
        public bool ActivateSettings { get; set; }
        public bool ForceUpdateSettings { get; set; }
            
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(RenewSettings)] = RenewSettings,
                [nameof(ActivateSettings)] = ActivateSettings,
                [nameof(ForceUpdateSettings)] = ForceUpdateSettings
            };
        }
    }
}
