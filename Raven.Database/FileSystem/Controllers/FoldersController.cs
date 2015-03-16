using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
	public class FoldersController : RavenFsApiController
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/folders/Subdirectories/{*directory}")]
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

            IEnumerable<string> result = Search.GetTermsFor("__directoryName", directory)
			                                .Where(subDir =>
			                                {
				                                if (subDir.StartsWith(directory) == false)
					                                return false;

				                                return nesting == subDir.Count(ch => ch == '/');
			                                })
			                                .Skip(Paging.Start)
			                                .Take(Paging.PageSize);

            return GetMessageWithObject(result)
                       .WithNoCache();
		}

		[HttpDelete]
		[RavenRoute("fs/{fileSystemName}/folder")]
		public HttpResponseMessage DeleteDirectory(string pathToDirectory)
		{
			pathToDirectory = FileHeader.Canonize(pathToDirectory);
			pathToDirectory += "/";

			Storage.Batch(accessor =>
			{
				var filesAndPages = accessor.GetFilesStartingWith(pathToDirectory, 0, Int32.MaxValue);
				DeleteFiles(filesAndPages, accessor, log);
			});

			FileSystem.Synchronizations.StartSynchronizeDestinationsInBackground();

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}