// -----------------------------------------------------------------------
//  <copyright file="LazyStartsWithOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.Document.Batches
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Client.Connection;
	using Raven.Client.Extensions;
	using Raven.Client.Shard;
	using Raven.Json.Linq;

	public class LazyStartsWithOperation<T> : ILazyOperation
	{
		private readonly string keyPrefix;

		private readonly string matches;

	    private readonly string exclude;

		private readonly int start;

		private readonly int pageSize;

		private readonly InMemoryDocumentSessionOperations sessionOperations;

		public LazyStartsWithOperation(string keyPrefix, string matches, string exclude, int start, int pageSize, InMemoryDocumentSessionOperations sessionOperations)
		{
			this.keyPrefix = keyPrefix;
			this.matches = matches;
		    this.exclude = exclude;
			this.start = start;
			this.pageSize = pageSize;
			this.sessionOperations = sessionOperations;
		}

		public GetRequest CreateRequest()
		{
			return new GetRequest
			{
				Url = "/docs",
				Query =
					string.Format(
						"startsWith={0}&matches={3}&exclude={4}&start={1}&pageSize={2}",
						Uri.EscapeDataString(keyPrefix),
						start.ToInvariantString(),
						pageSize.ToInvariantString(),
						Uri.EscapeDataString(matches ?? ""),
                        Uri.EscapeDataString(exclude ?? ""))
			};
		}

		public object Result { get; set; }

		public bool RequiresRetry { get; set; }

		public void HandleResponse(GetResponse response)
		{
			if (response.RequestHasErrors())
			{
				Result = null;
				RequiresRetry = false;
				return;
			}

			var jsonDocuments = SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)response.Result).OfType<RavenJObject>());

			Result = jsonDocuments
				.Select(sessionOperations.TrackEntity<T>)
				.ToArray();
		}
#if !SILVERLIGHT
		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			if (responses.Any(x => x.RequestHasErrors()))
			{
				Result = null;
				RequiresRetry = true;
				return;
			}

			var jsonDocuments = new List<JsonDocument>();
			foreach (var response in responses)
			{
				var documents = SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)response.Result).OfType<RavenJObject>()).ToArray();
				var duplicate = documents.FirstOrDefault(document => jsonDocuments.Any(x => x.Key == document.Key));
				if (duplicate != null)
				{
					throw new InvalidOperationException("Found document with id: " + duplicate.Key + " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
				}

				jsonDocuments.AddRange(documents);
			}

			Result = jsonDocuments
				.Select(sessionOperations.TrackEntity<T>)
				.ToArray();
		}
#endif
		public IDisposable EnterContext()
		{
			return null;
		}

#if !SILVERLIGHT
		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.StartsWith(keyPrefix, matches, start, pageSize)
				.Select(sessionOperations.TrackEntity<T>)
				.ToArray();
		}
#endif

		public void HandleEmbeddedResponse(object result)
		{
			Result = result;
		}
	}
}