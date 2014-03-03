using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class SearchController : RavenFsApiController
	{
		[HttpGet]
        [Route("ravenfs/{fileSystemName}/search/Terms")]		
		public string[] Terms()
		{
			IndexSearcher searcher;
			using (Search.GetSearcher(out searcher))
			{
				return searcher.IndexReader.GetFieldNames(IndexReader.FieldOption.ALL).ToArray();
			}
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/search")]		
		public SearchResults Get(string query, [FromUri] string[] sort)
		{
			int results;
			var keys = Search.Query(query, sort, Paging.Start, Paging.PageSize, out results);

			var list = new List<FileHeader>();

			Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

			return new SearchResults
			{
				Start = Paging.Start,
				PageSize = Paging.PageSize,
				Files = list,
				FileCount = results
			};
		}
	}
}
