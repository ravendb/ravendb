using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;

namespace Raven.Database.Server.Controllers
{
	public abstract class BundlesApiController : RavenApiController
	{
		public abstract string BundleName { get; }

		public override Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			if (DatabasesLandlord.SystemConfiguration.ActiveBundles.Contains(BundleName) == false)
			{
				throw new InvalidOperationException("Don't have the Bundle " + BundleName + "active");
			}

			return base.ExecuteAsync(controllerContext, cancellationToken);
		}
	}
}
