using System.Linq;
using System.Web.Http;
using Raven.Abstractions.RavenFS;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class StatsController : RavenFsApiController
	{
		[HttpGet]
        [Route("ravenfs/{fileSystemName}/stats")]
		public object Get()
		{
			var count = 0;
			Storage.Batch(accessor =>
			{
				count = accessor.GetFileCount();
			});

			return new FileSystemStats
			{
                Name = FileSystemName,
				FileCount = count,
                Metrics = RavenFileSystem.CreateMetrics(),
                ActiveSyncs = RavenFileSystem.SynchronizationTask.Queue.Active.ToList(),
                PendingSyncs = RavenFileSystem.SynchronizationTask.Queue.Pending.ToList()
			};
		}
	}
}