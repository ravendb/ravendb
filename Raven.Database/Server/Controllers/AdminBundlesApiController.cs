using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.Server.Controllers
{
	public abstract class AdminBundlesApiController : BaseAdminController
	{
		public abstract string BundleName { get; }

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var config = DatabasesLandlord.CreateTenantConfiguration(DatabaseName);
			if (config.ActiveBundles.Contains(BundleName) == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Could not figure out what to do"
				}, HttpStatusCode.BadRequest);
			}

			return await base.ExecuteAsync(controllerContext, cancellationToken);
		}
	}
}