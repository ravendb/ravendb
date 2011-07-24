#if !NET_3_5
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
	public static class AsyncDatabaseCommandsExtensions
	{
		public static Task<NameAndCount[]> GetTermsCount(this IAsyncDatabaseCommands cmds, string indexName, string field, string fromValue, int pageSize)
		{
			string[] terms = null;
			return cmds.GetTermsAsync(indexName, field, fromValue, pageSize)
				.ContinueWith(task =>
				{
					terms = task.Result;
					var termRequests = terms.Select(term => new IndexQuery
					{
						Query = field + ":" + RavenQuery.Escape(term),
						PageSize = 0,
					}.GetIndexQueryUrl("", indexName, "indexes"))
						.Select(url =>
						{
							var uri = new Uri(url);
							return new GetRequest
							{
								Url = uri.AbsolutePath,
								Query = uri.Query
							};
						})
						.ToArray();

					return cmds.MultiGetAsync(termRequests);
				})
				.Unwrap()
				.ContinueWith(task => task.Result.Select((t, i) => new NameAndCount
				{
					Count = RavenJObject.Parse(t.Result).Value<int>("TotalResults"),
					Name = terms[i]
				}).ToArray());
		}
	}

	public class NameAndCount
	{
		public string Name { get; set; }
		public int Count { get; set; }
	}
}
#endif