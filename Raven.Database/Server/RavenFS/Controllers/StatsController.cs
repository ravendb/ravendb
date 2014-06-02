using System.Linq;
using System.Web.Http;
using Raven.Database.Server.RavenFS.Extensions;
using System.Net.Http;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class StatsController : RavenFsApiController
	{
		[HttpGet]
        [Route("fs/{fileSystemName}/stats")]
        public HttpResponseMessage Get()
		{
			var count = 0;
			Storage.Batch(accessor =>
			{
				count = accessor.GetFileCount();
			});

            var stats = new FileSystemStats
            {
                Name = FileSystemName,
                FileCount = count,
                Metrics = RavenFileSystem.CreateMetrics(),
                ActiveSyncs = RavenFileSystem.SynchronizationTask.Queue.Active.ToList(),
                PendingSyncs = RavenFileSystem.SynchronizationTask.Queue.Pending.ToList()
            };

            return this.GetMessageWithObject(stats).WithNoCache();
		}
	}
}