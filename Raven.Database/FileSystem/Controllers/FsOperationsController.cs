using System.Net;
using System.Net.Http;
using System.Web.Http;

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
            if (status == null)
            {
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }

            lock (status.State)
            {
                return GetMessageWithObject(status);
            }
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
            if (status == null)
            {
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }

            lock (status.State)
            {
                return GetMessageWithObject(status);
            }
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/operations")]
        public HttpResponseMessage CurrentOperations()
        {
            return GetMessageWithObject(FileSystem.Tasks.GetAll());
        }
    }
};
