using System.Web.Http;
using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;

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
				FileCount = count
			};
		}
	}
}