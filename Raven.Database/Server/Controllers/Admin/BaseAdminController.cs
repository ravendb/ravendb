using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Threading.Tasks;
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

		public override async Task<HttpResponseMessage> ExecuteAsync(System.Web.Http.Controllers.HttpControllerContext controllerContext, System.Threading.CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
			var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

			HttpResponseMessage authMsg;
			if (authorizer.TryAuthorize(this, out authMsg) == false)
				return authMsg;

			var accessMode = DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode;
			if(accessMode == AnonymousUserAccessMode.Admin || accessMode == AnonymousUserAccessMode.All ||
			(accessMode == AnonymousUserAccessMode.Get && InnerRequest.Method.Method == "GET"))
				return await base.ExecuteAsync(controllerContext, cancellationToken);
				
			var user = authorizer.GetUser(this);
			if (user == null)
				return GetMessageWithObject(new
					{
						Error = "The operation '" + GetRequestUrl() + "' is only available to administrators, and could not find the user to authorize with"
					}, HttpStatusCode.Unauthorized);

			if (user.IsAdministrator(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode) == false &&
			user.IsAdministrator(Database) == false && SupportedByAnyAdditionalRoles(user) == false)
			{
				return GetMessageWithObject(new
					{
						Error = "The operation '" + GetRequestUrl() + "' is only available to administrators"
					}, HttpStatusCode.Unauthorized);
			}
			
			return await base.ExecuteAsync(controllerContext, cancellationToken);
		}

		private bool SupportedByAnyAdditionalRoles(IPrincipal user)
		{
			return AdditionalSupportedRoles.Any(role => user.IsInRole(DatabasesLandlord.SystemConfiguration.AnonymousUserAccessMode, role));
		}
	}
}