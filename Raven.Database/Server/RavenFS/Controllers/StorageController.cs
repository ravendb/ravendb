using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class StorageController : RavenFsApiController
	{
		[HttpPost]
		public Task CleanUp()
		{
			return StorageOperationsTask.CleanupDeletedFilesAsync();
		}

		[HttpPost]
		public Task RetryRenaming()
		{
			return StorageOperationsTask.ResumeFileRenamingAsync();
		}
	}
}