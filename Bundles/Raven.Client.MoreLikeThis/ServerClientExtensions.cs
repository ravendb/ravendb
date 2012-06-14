using System;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.MoreLikeThis
{
	public static class ServerClientExtensions
	{
		public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, new MoreLikeThisQueryParameters
			{
				DocumentId = documentId
			});
		}

		public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, MoreLikeThisQueryParameters parameters) where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, parameters);
		}


		public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId)
		{
			return MoreLikeThis<T>(advancedSession, index, new MoreLikeThisQueryParameters
			{
				DocumentId = documentId
			});
		}

		public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, MoreLikeThisQueryParameters parameters)
		{
			var cmd = advancedSession.DocumentStore.DatabaseCommands as ServerClient;
			if (cmd == null)
				throw new NotImplementedException("Embedded client isn't supported by the MoreLikeThis bundle");


			var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);

			// /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
			EnsureIsNotNullOrEmpty(index, "index");

			inMemoryDocumentSessionOperations.IncrementRequestCount();
			var multiLoadOperation = new MultiLoadOperation(inMemoryDocumentSessionOperations, cmd.DisableAllCaching);
			MultiLoadResult multiLoadResult;
			do
			{
				multiLoadOperation.LogOperation();
				using (multiLoadOperation.EnterMultiLoadContext())
				{
					var result = cmd.ExecuteGetRequest(parameters.GetRequestUri(index));

					multiLoadResult = ((RavenJObject)result).Deserialize<MultiLoadResult>(inMemoryDocumentSessionOperations.Conventions);
				}
			} while (multiLoadOperation.SetResult(multiLoadResult));

			return multiLoadOperation.Complete<T>();
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}
	}
}
