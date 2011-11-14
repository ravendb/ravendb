using System;
using System.Text;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Bundles.MoreLikeThis;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.MoreLikeThis
{
    public static class ServerClientExtensions
    {
        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId, MoreLikeThisQueryParameters parameters) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, documentId, null, parameters);
        }

        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, documentId, null, null);
        }

        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId, string fields, MoreLikeThisQueryParameters parameters) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, documentId, fields, parameters);
        }

        public static T[] MoreLikeThis<T, TIndexCreator>(this ISyncAdvancedSessionOperation advancedSession, string documentId, string fields) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return MoreLikeThis<T>(advancedSession, indexCreator.IndexName, documentId, fields, null);
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId, MoreLikeThisQueryParameters parameters)
        {
            return MoreLikeThis<T>(advancedSession, index, documentId, null, parameters);
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId)
        {
            return MoreLikeThis<T>(advancedSession, index, documentId, null, null);
        }

        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId, string fields, MoreLikeThisQueryParameters parameters)
        {
			var cmd = advancedSession.DatabaseCommands as ServerClient;
			if (cmd == null)
				throw new NotImplementedException("Embedded client isn't supported");

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

					var requestUri = GetRequestUri(index, documentId, fields, parameters);

					var result = cmd.ExecuteGetRequest(requestUri);

					multiLoadResult = RavenJObject.Parse(result).Deserialize<MultiLoadResult>(inMemoryDocumentSessionOperations.Conventions);
				}
			} while (multiLoadOperation.SetResult(multiLoadResult));

			return multiLoadOperation.Complete<T>();
        }

        private static void EnsureIsNotNullOrEmpty(string key, string argName)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", argName);
        }

        private static string GetRequestUri(string index, string documentId, string fields, MoreLikeThisQueryParameters parameters)
        {
            var hasParams = CheckIfHasParameters(fields, parameters);
            var uri = new StringBuilder();
            uri.AppendFormat("/morelikethis/{0}/{1}", Uri.EscapeUriString(index), documentId);
            if (!hasParams) return uri.ToString();
            uri.Append("?");
            if (!String.IsNullOrEmpty(fields)) 
                uri.AppendFormat("fields={0}&", fields);
            if (parameters != null)
            {
                if (parameters.Boost != null && parameters.Boost != MoreLikeThisQueryParameters.DefaultBoost)
                    uri.Append("boost=true&");
                if (parameters.MaximumQueryTerms != null &&
                    parameters.MaximumQueryTerms != MoreLikeThisQueryParameters.DefaultMaximumQueryTerms)
                    uri.AppendFormat("maxQueryTerms={0}&", parameters.MaximumQueryTerms);
                if (parameters.MaximumNumberOfTokensParsed != null &&
                    parameters.MaximumNumberOfTokensParsed !=
                    MoreLikeThisQueryParameters.DefaultMaximumNumberOfTokensParsed)
                    uri.AppendFormat("maxNumTokens={0}&", parameters.MaximumNumberOfTokensParsed);
                if (parameters.MaximumWordLength != null &&
                    parameters.MaximumWordLength != MoreLikeThisQueryParameters.DefaultMaximumWordLength)
                    uri.AppendFormat("maxWordLen={0}", parameters.MaximumWordLength);
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
            }
            var ret = uri.ToString();
            return ret.Substring(0, ret.Length - 1);
        }

        private static bool CheckIfHasParameters(string fields, MoreLikeThisQueryParameters parameters)
        {
            return !String.IsNullOrEmpty(fields) || (parameters != null && (
                (parameters.Boost != null && parameters.Boost != MoreLikeThisQueryParameters.DefaultBoost) ||
                (parameters.MaximumQueryTerms != null && parameters.MaximumQueryTerms != MoreLikeThisQueryParameters.DefaultMaximumQueryTerms) ||
                (parameters.MaximumWordLength != null && parameters.MaximumWordLength != MoreLikeThisQueryParameters.DefaultMaximumWordLength) ||
                (parameters.MinimumDocumentFrequency != null && parameters.MinimumDocumentFrequency != MoreLikeThisQueryParameters.DefaltMinimumDocumentFrequency) ||
                (parameters.MinimumTermFrequency != null && parameters.MinimumTermFrequency != MoreLikeThisQueryParameters.DefaultMinimumTermFrequency) ||
                (parameters.MinimumWordLength != null && parameters.MinimumWordLength != MoreLikeThisQueryParameters.DefaultMinimumWordLength) ||
                (parameters.MaximumNumberOfTokensParsed != null && parameters.MaximumNumberOfTokensParsed != MoreLikeThisQueryParameters.DefaultMaximumNumberOfTokensParsed) ||
                parameters.StopWordsDocumentId != null));
        }
    }
}
