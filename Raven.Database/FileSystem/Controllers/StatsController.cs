using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.FileSystem;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
	public class StatsController : RavenFsApiController
	{
		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/stats")]
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