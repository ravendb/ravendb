#if !NET35
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
							var uriParts = url.Split(new[] {'?'}, StringSplitOptions.RemoveEmptyEntries);
							return new GetRequest
							{
								Url = uriParts[0],
								Query = uriParts[1]
							};
						})
						.ToArray();

					if (termRequests.Length == 0)
						return Task.Factory.StartNew(() => new NameAndCount[0]);

					return cmds.MultiGetAsync(termRequests)
						.ContinueWith(termsResultsTask => termsResultsTask.Result.Select((t, i) => new NameAndCount
						{
							Count = t.Result.Value<int>("TotalResults"),
							Name = terms[i]
						}).ToArray());
				})
				.Unwrap();
		}
	}

	public class NameAndCount
	{
		public string Name { get; set; }
		public int Count { get; set; }
	}
}
#endif