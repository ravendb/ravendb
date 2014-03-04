using System.Web.Http;

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

			return new Stats
			{
				FileCount = count
			};
		}

		public class Stats
		{
			public int FileCount;
		}
	}
}