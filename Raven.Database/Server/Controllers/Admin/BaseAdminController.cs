using System;
using System.Net.Http;
using System.Security;
using System.Security.Principal;
using System.ServiceModel.Security;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server.Controllers.Admin
{
	public abstract class BaseAdminController : RavenApiController
	{
		public override Task<HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
		{
			IPrincipal user;
			var controller = new AdminController { Request = controllerContext.Request, Configuration = controllerContext.Configuration, ControllerContext = controllerContext};
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			user = authorizer.GetUser(controller);

			if (user == null)
				return null;
			if (user.IsInRole("Admin"))
				return base.ExecuteAsync(controllerContext, cancellationToken);

			throw new SecurityException("Must be admin to access this section");
			// TODO: Verify user is admin
			if (DateTime.Today > new DateTime(2013, 8, 25))
				throw new ExpiredSecurityTokenException("HACK EXPIRED error");
			return base.ExecuteAsync(controllerContext, cancellationToken);
		}

		//[SystemRoute("admin/databases/{id}")]
		//[DatabaseRoute("docs")] // /docs AND /databases/foo/docs
	}
}
