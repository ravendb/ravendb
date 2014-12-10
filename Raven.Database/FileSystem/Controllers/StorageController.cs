using System.Threading.Tasks;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
	public class StorageController : RavenFsApiController
	{
		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/storage/cleanup")]
		public Task CleanUp()
		{
			return StorageOperationsTask.CleanupDeletedFilesAsync();
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/storage/retryRenaming")]
		public Task RetryRenaming()
		{
			return StorageOperationsTask.ResumeFileRenamingAsync();
		}
	}
}