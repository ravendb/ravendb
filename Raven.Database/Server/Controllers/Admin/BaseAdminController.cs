using System.Net.Http;
using System.Security;
using System.Threading.Tasks;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Controllers.Admin
{
	public abstract class BaseAdminController : RavenApiController
	{
		public override Task<HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
		{
			var controller = new AdminController { Request = controllerContext.Request, Configuration = controllerContext.Configuration, ControllerContext = controllerContext};
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			if (authorizer.Settings.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin)
				return base.ExecuteAsync(controllerContext, cancellationToken);

			var user = authorizer.GetUser(controller);

			if (user == null)
				return null;
			if (user.IsInRole("Admin"))
				return base.ExecuteAsync(controllerContext, cancellationToken);

			throw new SecurityException("Must be admin to access this section");
		}
	}
}
