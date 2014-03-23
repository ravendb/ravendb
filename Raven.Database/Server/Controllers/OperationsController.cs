using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class OperationsController : RavenDbApiController
	{
		[HttpGet]
		[Route("operation/status")]
		[Route("databases/{databaseName}/operation/status")]
		public HttpResponseMessage OperationStatusGet()
		{
			var idStr = GetQueryStringValue("id");
			long id;
			if (long.TryParse(idStr, out id) == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Query string variable id must be a valid int64"
				}, HttpStatusCode.BadRequest);
			}

			var status = Database.Tasks.GetTaskState(id);
			return status == null ? GetEmptyMessage(HttpStatusCode.NotFound) : GetMessageWithObject(status);
		}
	}
}