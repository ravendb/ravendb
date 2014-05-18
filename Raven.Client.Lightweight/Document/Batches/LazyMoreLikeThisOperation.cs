// -----------------------------------------------------------------------
//  <copyright file="LazyMoreLikeThisOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Shard;
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
	public class LazyMoreLikeThisOperation<T> : ILazyOperation
	{
		private readonly MultiLoadOperation multiLoadOperation;
		private readonly MoreLikeThisQuery query;

		public LazyMoreLikeThisOperation(MultiLoadOperation multiLoadOperation, MoreLikeThisQuery query)
		{
			this.multiLoadOperation = multiLoadOperation;
			this.query = query;
		}

		public GetRequest CreateRequest()
		{
			var uri = query.GetRequestUri();

			var separator = uri.IndexOf('?');

			return new GetRequest()
			{
				Url = uri.Substring(0, separator),
				Query = uri.Substring(separator + 1, uri.Length - separator - 1)
			};
		}

		public object Result { get; private set; }
		public QueryResult QueryResult { get; set; }
		public bool RequiresRetry { get; private set; }

		public void HandleResponse(GetResponse response)
		{
			var result = response.Result;

			var multiLoadResult = new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
			};

			HandleResponse(multiLoadResult);
		}

		private void HandleResponse(MultiLoadResult multiLoadResult)
		{
			RequiresRetry = multiLoadOperation.SetResult(multiLoadResult);
			if (RequiresRetry == false)
				Result = multiLoadOperation.Complete<T>();
		}

		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var list = new List<MultiLoadResult>(
				from response in responses
				let result = response.Result
				select new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				});

			var capacity = list.Max(x => x.Results.Count);

			var finalResult = new MultiLoadResult
			{
				Includes = new List<RavenJObject>(),
				Results = new List<RavenJObject>(Enumerable.Range(0, capacity).Select(x => (RavenJObject)null))
			};


			foreach (var multiLoadResult in list)
			{
				finalResult.Includes.AddRange(multiLoadResult.Includes);

				for (int i = 0; i < multiLoadResult.Results.Count; i++)
				{
					if (finalResult.Results[i] == null)
						finalResult.Results[i] = multiLoadResult.Results[i];
				}
			}
			RequiresRetry = multiLoadOperation.SetResult(finalResult);
			if (RequiresRetry == false)
				Result = multiLoadOperation.Complete<T>();
		}

		public IDisposable EnterContext()
		{
			return multiLoadOperation.EnterMultiLoadContext();
		}

		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.MoreLikeThis(query);
		}

		public void HandleEmbeddedResponse(object result)
		{
			HandleResponse((MultiLoadResult)result);
		}
	}
}