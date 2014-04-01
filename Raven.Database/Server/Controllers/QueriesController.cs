using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Web.Http;
using Amazon.EC2.Model;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class QueriesController : RavenDbApiController
	{
		[HttpGet]
		[Route("queries")]
		[Route("databases/{databaseName}/queries")]
		public Task<HttpResponseMessage> QueriesGet()
		{
			return GetQueriesResponse(true);
		}

		[HttpPost]
		[Route("queries")]
		[Route("databases/{databaseName}/queries")]
		public Task<HttpResponseMessage> QueriesPost()
		{
			return GetQueriesResponse(false);
		}

		private async Task<HttpResponseMessage> GetQueriesResponse(bool isGet)
		{
			RavenJArray itemsToLoad;
		    if (isGet == false)
		    {
		        itemsToLoad = await ReadJsonArrayAsync();
                AddRequestTraceInfo(sb =>
                {
                    foreach (var item in itemsToLoad)
                    {
                        sb.Append("\t").Append(item).AppendLine();
                    }
                });
		    }
		    else
		    {
		        itemsToLoad = new RavenJArray(GetQueryStringValues("id").Cast<object>());
		    }

			var result = new MultiLoadResult();
			var loadedIds = new HashSet<string>();
			var includes = GetQueryStringValues("include") ?? new string[0];
			var transformer = GetQueryStringValue("transformer") ?? GetQueryStringValue("resultTransformer");
			var queryInputs = ExtractQueryInputs();
			var transactionInformation = GetRequestTransaction();
			var includedEtags = new List<byte>();

			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (RavenJToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if (loadedIds.Add(value) == false)
						continue;
					var documentByKey = string.IsNullOrEmpty(transformer)
										? Database.Documents.Get(value, transactionInformation)
										: Database.Documents.GetWithTransformer(value, transformer, transactionInformation, queryInputs);
				    if (documentByKey == null)
				    {
                        if(ClientIsV3OrHigher)
                            result.Results.Add(null); 
                        continue;
				    }
					result.Results.Add(documentByKey.ToJson());

					if (documentByKey.Etag != null)
						includedEtags.AddRange(documentByKey.Etag.ToByteArray());

					includedEtags.Add((documentByKey.NonAuthoritativeInformation ?? false) ? (byte)0 : (byte)1);
				}

				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc) =>
				{
					includedEtags.AddRange(etag.ToByteArray());
					result.Includes.Add(includedDoc);
				}, includes, loadedIds);

				foreach (var item in result.Results.Where(item => item != null))
				{
					addIncludesCommand.Execute(item);
				}
			});

			Etag computedEtag;

			using (var md5 = MD5.Create())
			{
				var computeHash = md5.ComputeHash(includedEtags.ToArray());
				computedEtag = Etag.Parse(computeHash);
			}

			if (MatchEtag(computedEtag))
			{
				return GetEmptyMessage(HttpStatusCode.NotModified);
			}

			var msg = GetMessageWithObject(result);
			WriteETag(computedEtag, msg);

			AddRequestTraceInfo(sb => sb.Append("Results count: {0}, includes count: {1}", result.Results.Count, result.Includes.Count).AppendLine());

			return msg;
		}
        
	}
}