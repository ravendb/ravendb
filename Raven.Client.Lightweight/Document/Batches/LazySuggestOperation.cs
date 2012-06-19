using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Policy;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Shard;
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
#if !NET35

	public class LazySuggestOperation : ILazyOperation
	{
		private readonly string index;
		private readonly SuggestionQuery suggestionQuery;

		public LazySuggestOperation(string index, SuggestionQuery suggestionQuery)
		{
			this.index = index;
			this.suggestionQuery = suggestionQuery;
		}

		public GetRequest CraeteRequest()
		{
			return new GetRequest
			{
				Url = "/suggest/" + index,
				Query = string.Format("term={0}&field={1}&max={2}&distance={3}&accuracy={4}",
									  suggestionQuery.Term,
									  suggestionQuery.Field,
									  suggestionQuery.MaxSuggestions,
									  suggestionQuery.Distance,
									  suggestionQuery.Accuracy.ToString(CultureInfo.InvariantCulture))
			};
		}

		public object Result { get; private set; }
		public bool RequiresRetry { get; private set; }
		public void HandleResponse(GetResponse response)
		{
			if (response.Status != 200)
			{
				throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" +
													response.Result);
			}

			var result = (RavenJObject)response.Result;
			Result = new SuggestionQueryResult
			{
				Suggestions = ((RavenJArray)result["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
			};
		}

		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var result = new SuggestionQueryResult
			{
				Suggestions = (from item in responses
							   let data = (RavenJObject)item.Result
							   from suggestion in (RavenJArray)data["Suggestions"]
							   select suggestion.Value<string>())
							  .Distinct()
							  .ToArray()
			};

			Result = result;
		}

		public IDisposable EnterContext()
		{
			return null;
		}

		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.Suggest(index, suggestionQuery);
		}

		public void HandleEmbeddedResponse(object result)
		{
			Result = result;
		}
	}
#endif
}