using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
    [RoutePrefix("")]
    public class FsOperationsController : BaseFileSystemApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/operation/status")]
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

            var status = FileSystem.Tasks.GetTaskState(id);
            return GetOperationStatusMessage(status);
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/operation/kill")]
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
            var status = FileSystem.Tasks.KillTask(id);
            return GetOperationStatusMessage(status);
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/operations")]
        public HttpResponseMessage CurrentOperations()
        {
            return GetMessageWithObject(FileSystem.Tasks.GetAll());
        }

        private HttpResponseMessage GetOperationStatusMessage(IOperationState status)
        {
            if (status == null)
            {
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }


            if (status.State != null)
            {
                lock (status.State)
                {
                    return GetMessageWithObject(status);
                }
            }
            return GetMessageWithObject(status);
        }
    }
};
