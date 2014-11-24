using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.FileSystem.Controllers
{
	public class StorageController : RavenFsApiController
	{
		[HttpPost]
        [Route("fs/{fileSystemName}/storage/cleanup")]
		public Task CleanUp()
		{
			return StorageOperationsTask.CleanupDeletedFilesAsync();
		}

		[HttpPost]
        [Route("fs/{fileSystemName}/storage/retryRenaming")]
		public Task RetryRenaming()
		{
			return StorageOperationsTask.ResumeFileRenamingAsync();
		}
	}
}