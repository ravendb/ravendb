using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web.Http;
using Lucene.Net.Index;
using Lucene.Net.Search;
using System.Net.Http;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
	public class SearchController : RavenFsApiController
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/search/Terms")]
        public HttpResponseMessage Terms([FromUri] string query  = "")
        {
            IndexSearcher searcher;
            using (Search.GetSearcher(out searcher))
            {
                string[] result = searcher.IndexReader.GetFieldNames(IndexReader.FieldOption.ALL)
                                    .Where(x => x.IndexOf(query, 0, StringComparison.InvariantCultureIgnoreCase) != -1).ToArray();

                return this.GetMessageWithObject(result);
            }
        }

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/search")]
        public HttpResponseMessage Get(string query, [FromUri] string[] sort)
		{
			int results;
			var keys = Search.Query(query, sort, Paging.Start, Paging.PageSize, out results);

            var list = new List<FileHeader>();

			Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

			var result = new SearchResults
			{
				Start = Paging.Start,
				PageSize = Paging.PageSize,
				Files = list,
				FileCount = results
			};

            return this.GetMessageWithObject(result);
		}

		[HttpDelete]
		[RavenRoute("fs/{fileSystemName}/search")]
		public HttpResponseMessage DeleteByQuery(string query, [FromUri] string[] sort)
		{
			int results;
			var keys = Search.Query(query, sort, Paging.Start, Paging.PageSize, out results);

			Storage.Batch(accessor =>
			{
				var files = keys.Select(accessor.ReadFile);
				DeleteFiles(files, accessor, log);
			});

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}
