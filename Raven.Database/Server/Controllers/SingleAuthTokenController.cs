using System.Net.Http;
using System.Web.Http;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Controllers
{
	public class SingleAuthTokenController : RavenDbApiController
	{
		[HttpGet]
		[Route("singleAuthToken")]
		[Route("databases/{databaseName}/singleAuthToken")]
		public HttpResponseMessage SingleAuthGet()
		{
			var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

			var token = authorizer.GenerateSingleUseAuthToken(Database, User, this);
			
			return GetMessageWithObject(new
			{
				Token = token
			});
		}
	}
}
