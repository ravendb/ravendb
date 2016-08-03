using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
    public class FoldersController : BaseFileSystemApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/folders/Subdirectories/{*directory}")]
        public HttpResponseMessage Subdirectories(string directory = null)
        {

            bool startsWith;
            bool.TryParse(GetQueryStringValue("startsWith"), out startsWith);

            int nesting = 1;

            if (directory != null)
            {
                if (directory.StartsWith("/") == false)
                    directory = "/" + directory;

                if (startsWith == false && directory.EndsWith("/") == false)
                {
                    directory = directory + '/';
                }

                nesting = directory.Count(ch => ch == '/');

            }
            else
            {
                directory = "/";
            }

            IEnumerable<string> result = Search.GetTermsFor("__directoryName", directory)
                                            .Where(subDir =>
                                            {
                                                if (startsWith == false && subDir == directory) // exclude self when searching for children
                                                    return false;

                                                if (subDir.StartsWith(directory) == false)
                                                    return false;

                                                return nesting == subDir.Count(ch => ch == '/');
                                            })
                                            .Skip(Paging.Start)
                                            .Take(Paging.PageSize);

            return GetMessageWithObject(result)
                       .WithNoCache();
        }
    }
}
