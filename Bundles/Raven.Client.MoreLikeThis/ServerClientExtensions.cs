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
			var cmd = advancedSession.DatabaseCommands as ServerClient;
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
					var requestUri = GetRequestUri(index, parameters);

					var result = cmd.ExecuteGetRequest(requestUri);

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

		private static string GetRequestUri(string index, MoreLikeThisQueryParameters parameters)
		{
			var uri = new StringBuilder();
			uri.AppendFormat("/morelikethis/{0}/{1}?", Uri.EscapeUriString(index), Uri.EscapeUriString(parameters.DocumentId));
			if (parameters.Fields != null)
			{
				foreach (var field in parameters.Fields)
				{
					uri.AppendFormat("fields={0}&", field);
				}
			}
			if (parameters.Boost != null && parameters.Boost != MoreLikeThisQueryParameters.DefaultBoost)
				uri.Append("boost=true&");
			if (parameters.MaximumQueryTerms != null &&
			    parameters.MaximumQueryTerms != MoreLikeThisQueryParameters.DefaultMaximumQueryTerms)
				uri.AppendFormat("maxQueryTerms={0}&", parameters.MaximumQueryTerms);
			if (parameters.MaximumNumberOfTokensParsed != null &&
			    parameters.MaximumNumberOfTokensParsed != MoreLikeThisQueryParameters.DefaultMaximumNumberOfTokensParsed)
				uri.AppendFormat("maxNumTokens={0}&", parameters.MaximumNumberOfTokensParsed);
			if (parameters.MaximumWordLength != null &&
			    parameters.MaximumWordLength != MoreLikeThisQueryParameters.DefaultMaximumWordLength)
				uri.AppendFormat("maxWordLen={0}&", parameters.MaximumWordLength);
			if (parameters.MinimumDocumentFrequency != null &&
			    parameters.MinimumDocumentFrequency != MoreLikeThisQueryParameters.DefaltMinimumDocumentFrequency)
				uri.AppendFormat("minDocFreq={0}&", parameters.MinimumDocumentFrequency);
			if (parameters.MinimumTermFrequency != null &&
			    parameters.MinimumTermFrequency != MoreLikeThisQueryParameters.DefaultMinimumTermFrequency)
				uri.AppendFormat("minTermFreq={0}&", parameters.MinimumTermFrequency);
			if (parameters.MinimumWordLength != null &&
			    parameters.MinimumWordLength != MoreLikeThisQueryParameters.DefaultMinimumWordLength)
				uri.AppendFormat("minWordLen={0}&", parameters.MinimumWordLength);
			if (parameters.StopWordsDocumentId != null)
				uri.AppendFormat("stopWords={0}&", parameters.StopWordsDocumentId);
			return uri.ToString();
		}
	}
}
