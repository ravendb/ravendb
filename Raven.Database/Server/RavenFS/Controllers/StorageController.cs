using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class StorageController : RavenFsApiController
	{
		[HttpPost]
        [Route("ravenfs/{fileSystemName}/storage/cleanup")]
		public Task CleanUp()
		{
			return StorageOperationsTask.CleanupDeletedFilesAsync();
		}

		[HttpPost]
        [Route("ravenfs/{fileSystemName}/storage/retryRenaming")]
		public Task RetryRenaming()
		{
			return StorageOperationsTask.ResumeFileRenamingAsync();
		}
	}
}