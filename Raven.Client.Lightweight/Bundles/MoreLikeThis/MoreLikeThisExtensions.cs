using System;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;

namespace Raven.Client.Bundles.MoreLikeThis
{
	public static class MoreLikeThisExtensions
	{
		public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, new MoreLikeThisQuery
			{
				DocumentId = documentId
			});
		}

		public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, MoreLikeThisQuery parameters) where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, parameters);
		}


		public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId)
		{
			return MoreLikeThis<T>(advancedSession, index, new MoreLikeThisQuery
			{
				DocumentId = documentId
			});
		}

		public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, MoreLikeThisQuery parameters)
		{
			if (string.IsNullOrEmpty(index))
				throw new ArgumentException("Index name cannot be null or empty", "index");

			parameters.IndexName = index;

			// /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
			var cmd = advancedSession.DocumentStore.DatabaseCommands;

			var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)advancedSession);
			inMemoryDocumentSessionOperations.IncrementRequestCount();

			var multiLoadOperation = new MultiLoadOperation(inMemoryDocumentSessionOperations, cmd.DisableAllCaching, null, null);
			MultiLoadResult multiLoadResult;
			do
			{
				multiLoadOperation.LogOperation();
				using (multiLoadOperation.EnterMultiLoadContext())
				{
					multiLoadResult = cmd.MoreLikeThis(parameters);
				}
			} while (multiLoadOperation.SetResult(multiLoadResult));

			return multiLoadOperation.Complete<T>();
		}
	}
}