using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;

namespace Raven.Database.Server.Controllers
{
    public abstract class BundlesApiController : BaseDatabaseApiController
    {
        public abstract string BundleName { get; }

        public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
        {
            InnerInitialization(controllerContext);
            HttpResponseMessage msg;
            if (IsClientV4OrHigher(out msg))
                return msg;

            DocumentDatabase db;
            try
            {
                db = await DatabasesLandlord.GetResourceInternal(DatabaseName).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                return GetMessageWithObject(new
                {
                    Error = "Could not open database named: " + DatabaseName + ", " + e.Message
                }, HttpStatusCode.ServiceUnavailable);
            }
            if (db == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "Could not open database named: " + DatabaseName + ", database does not exists" 
                }, HttpStatusCode.ServiceUnavailable);
            }
            if (db.Configuration == null || db.Configuration.ActiveBundles == null ||
                !db.Configuration.ActiveBundles.Any(activeBundleName => activeBundleName.Equals(BundleName, StringComparison.InvariantCultureIgnoreCase)))
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
