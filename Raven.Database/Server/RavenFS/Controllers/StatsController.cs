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
                Metrics = FileSystem.CreateMetrics(),
                ActiveSyncs = FileSystem.SynchronizationTask.Queue.Active.ToList(),
                PendingSyncs = FileSystem.SynchronizationTask.Queue.Pending.ToList()
            };

            return this.GetMessageWithObject(stats).WithNoCache();
		}
	}
}