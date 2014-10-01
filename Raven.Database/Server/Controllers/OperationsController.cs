using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using System.Linq;

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

        [HttpGet]
        [Route("operation/kill")]
        [Route("databases/{databaseName}/operation/kill")]
        public HttpResponseMessage OperationKill()
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
            var status = Database.Tasks.KillTask(id);
            return status == null ? GetEmptyMessage(HttpStatusCode.NotFound) : GetMessageWithObject(status);
        }

        [HttpGet]
        [Route("operations")]
        [Route("databases/{databaseName}/operations")]
        public HttpResponseMessage CurrentOperations()
        {
            return GetMessageWithObject(Database.Tasks.GetAll());
        }

        [HttpGet]
        [Route("operation/alerts")]
        [Route("databases/{databaseName}/operation/alerts")]
        public HttpResponseMessage Alerts()
        {
            const int FreeThreshold = 15;
            var drives = DriveInfo.GetDrives();
            string[] alerts = drives
                .Where(x => x.DriveType == DriveType.Fixed && x.TotalFreeSpace*1.0 / x.TotalSize < FreeThreshold / 100.0)
                .Select(x => string.Format("Database disk ({0}) size has less than {1}% free.", x.Name, FreeThreshold))
                .ToArray();
            return GetMessageWithObject(alerts);
        }
	}
}