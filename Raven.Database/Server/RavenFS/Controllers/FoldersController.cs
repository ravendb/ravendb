using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class FoldersController : RavenFsApiController
	{
		[HttpGet]
        [Route("ravenfs/{fileSystemName}/folders/Subdirectories/{*directory}")]
		public IEnumerable<string> Subdirectories(string directory = null)
		{
			var add = directory == null ? 0 : 1;
			directory = "/" + directory;
			var nesting = directory.Count(ch => ch == '/') + add;
			return Search.GetTermsFor("__directory", directory)
				.Where(subDir =>
				{
					if (subDir.StartsWith(directory) == false)
						return false;

					return nesting == subDir.Count(ch => ch == '/');
				})
				.Skip(Paging.Start)
				.Take(Paging.PageSize);
		}
	}
}