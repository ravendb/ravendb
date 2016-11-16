using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.Server.Controllers
{
    public abstract class AdminBundlesApiController : BaseAdminDatabaseApiController
    {
        public abstract string BundleName { get; }

        public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
        {
            InnerInitialization(controllerContext);

            HttpResponseMessage msg;
            if (IsClientV4OrHigher(out msg))
                return msg;

            var config = DatabasesLandlord.CreateTenantConfiguration(DatabaseName);
            if (config == null || config.ActiveBundles == null ||
                !config.ActiveBundles.Any(activeBundleName => activeBundleName.Equals(BundleName, StringComparison.InvariantCultureIgnoreCase)))
            {
                return GetMessageWithObject(new
                {
                    Error = BundleName + " bundle not activated in database named: " + DatabaseName
                }, HttpStatusCode.BadRequest);
            }

            return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);
        }
    }
}
