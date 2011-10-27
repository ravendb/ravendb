using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Client.MoreLikeThis
{
    public static class ServerClientExtensions
    {
        public static IEnumerable<T> MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId, string fields)
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
					
					var requestUri = string.Format("/morelikethis/{0}/{1}?fields={2}",
															 Uri.EscapeUriString(index),
															 documentId,
															 Uri.EscapeDataString(fields));

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
    }
}
