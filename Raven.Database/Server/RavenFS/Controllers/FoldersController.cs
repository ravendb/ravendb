using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class FoldersController : RavenFsApiController
	{
		[HttpGet]
        [Route("fs/{fileSystemName}/folders/Subdirectories/{*directory}")]
        public HttpResponseMessage Subdirectories(string directory = null)
		{
            int nesting = 1;
            if (directory != null)
            {
                directory = directory.Trim('/');

                directory = "/" + directory;
                nesting = directory.Count(ch => ch == '/') + 1;
            }
            else
            {
                directory = "/";
            }

            IEnumerable<string> result = Search.GetTermsFor("__directory", directory)
			                                .Where(subDir =>
			                                {
				                                if (subDir.StartsWith(directory) == false)
					                                return false;

				                                return nesting == subDir.Count(ch => ch == '/');
			                                })
			                                .Skip(Paging.Start)
			                                .Take(Paging.PageSize);

            return this.GetMessageWithObject(result);
		}
	}
}