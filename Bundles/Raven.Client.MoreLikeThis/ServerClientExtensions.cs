using System;
using Newtonsoft.Json;
using Raven.Client.Connection;

namespace Raven.Client.MoreLikeThis
{
    public static class ServerClientExtensions
    {
        public static T[] MoreLikeThis<T>(this ISyncAdvancedSessionOperation advancedSession, string index, string documentId, string fields)
        {
			var cmd = advancedSession.DatabaseCommands as ServerClient;
			if (cmd == null)
				throw new NotImplementedException("Embedded client isn't supported");

			// /morelikethis/(index-name)/(ravendb-document-id)?fields=(fields)
			EnsureIsNotNullOrEmpty(index, "index");

			var requestUri = string.Format("/morelikethis/{0}/{1}?fields={2}",
													 Uri.EscapeUriString(index),
													 documentId,
													 Uri.EscapeDataString(fields));

			var result = cmd.ExecuteGetRequest(requestUri);
			return JsonConvert.DeserializeObject<T[]>(result);
        }

        private static void EnsureIsNotNullOrEmpty(string key, string argName)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", argName);
        }
    }
}
