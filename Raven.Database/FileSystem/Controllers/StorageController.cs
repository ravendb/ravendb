using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
    public class StorageController : BaseFileSystemApiController
    {
        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/storage/cleanup")]
        public Task CleanUp()
        {
            return Files.CleanupDeletedFilesAsync();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/storage/retryRenaming")]
        public Task RetryRenaming()
        {
            return Files.ResumeFileRenamingAsync();
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/storage/retryCopying")]
        public Task RetryCopying()
        {
            return Files.ResumeFileCopyingAsync();
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/storage/debug/esent/current-autoincrement-table-values")]
        public HttpResponseMessage EsentCurrentAutoincrementTableValues()
        {
            Dictionary<string, long> result = new Dictionary<string, long>();

            Storage.Batch(x => result = x.Esent_GetCurrentAutoIncrementValues());

            return GetMessageWithObject(result, HttpStatusCode.OK)
                .WithNoCache();
        }
    }
}
