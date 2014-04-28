using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Database.Server.RavenFS.Storage;
using System.Net.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class SearchController : RavenFsApiController
	{
        [HttpGet]
        [Route("ravenfs/{fileSystemName}/search/Terms")]
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
        [Route("ravenfs/{fileSystemName}/search")]
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
	}
}
