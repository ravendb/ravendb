using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Server.Security;

namespace Raven.Database.Server.Controllers
{
	public class SingleAuthTokenController : RavenApiController
	{
		[HttpGet("singleAuthToken")]
		[HttpGet("databases/{databaseName}/singleAuthToken")]
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
