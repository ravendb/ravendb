using System;
using Raven.Client.Connection;

namespace Raven.Client.MoreLikeThis
{
    public static class ServerClientExtensions
    {
        public static string MoreLikeThis(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId, string fields)
        {
            return advancedSession.DatabaseCommands.MoreLikeThis(index, documentId, fields);
        }


        public static string MoreLikeThis(this IDatabaseCommands commands, string index, string documentId, string fields)
        {
            if (commands is ServerClient) return (commands as ServerClient).MoreLikeThis(index, documentId, fields);
            //if (commands is EmbeddableDocumentStore) return (commands as EmbeddableDocumentStore).MoreLikeThis(index, documentId, fields);
            throw new NotImplementedException();
        }

        public static string MoreLikeThis(this ServerClient server, string index, string documentId, string fields)
        {
            // /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
            EnsureIsNotNullOrEmpty(index, "index");

            var requestUri = string.Format("/morelikethis/{0}/{1}?fields={2}",
				                                     Uri.EscapeUriString(index),
				                                     documentId,
				                                     Uri.EscapeDataString(fields));

            return server.ExecuteGetRequest(requestUri);

            //return SerializationHelper.ToQueryResult(json, request.ResponseHeaders["ETag"]);
        }

		//public static string MoreLikeThis(this EmbeddedDatabaseCommands server, string index, string documentId, string fields)
		//{
		//    throw new NotImplementedException();
		//}

        private static void EnsureIsNotNullOrEmpty(string key, string argName)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", argName);
        }
    }
}
