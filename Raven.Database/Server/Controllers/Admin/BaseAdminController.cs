using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Security.Principal;
using System.Threading.Tasks;
using Amazon.ElastiCache.Model;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Controllers.Admin
{
	public abstract class BaseAdminController : RavenApiController
	{
		protected virtual WindowsBuiltInRole[] AdditionalSupportedRoles
		{
			get
			{
				return new WindowsBuiltInRole[0];
			}
		}

		public override Task<HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
		{
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
			var user = authorizer.GetUser(this);
			if (user == null)
				return null;

			if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false &&
			user.IsAdministrator(Database) == false && SupportedByAnyAdditionalRoles(user) == false)
			{
				return new CompletedTask<HttpResponseMessage>(GetMessageWithObject(
					new
					{
						Error = "The operation '" + GetRequestUrl() + "' is only available to administrators"
					}, HttpStatusCode.Unauthorized));
			}
			
			return base.ExecuteAsync(controllerContext, cancellationToken);
		}

		private bool SupportedByAnyAdditionalRoles(IPrincipal user)
		{
			return AdditionalSupportedRoles.Any(role => user.IsInRole(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode, role));
		}
	}
}