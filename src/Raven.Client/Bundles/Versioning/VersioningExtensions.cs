
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Document.Async;

namespace Raven.Client.Bundles.Versioning
{
    public static class VersioningExtensions
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static T[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
        {
            var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)session);
            var jsonDocuments = ((DocumentSession)session).DatabaseCommands.GetRevisionsFor(id, start, pageSize);
            return jsonDocuments
                .Select(inMemoryDocumentSessionOperations.TrackEntity<T>)
                .ToArray();
        }

        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static async Task<T[]> GetRevisionsForAsync<T>(this IAsyncAdvancedSessionOperations session, string id, int start = 0, int pageSize = 25)
        {
            var inMemoryDocumentSessionOperations = (InMemoryDocumentSessionOperations)session;
            var jsonDocuments = await ((AsyncDocumentSession)session).AsyncDatabaseCommands.GetRevisionsForAsync(id, start, pageSize).ConfigureAwait(false);
            return jsonDocuments
             .Select(x => (T)inMemoryDocumentSessionOperations.ConvertToEntity(typeof(T),x.Key + "/__revisions", x.DataAsJson, x.Metadata))
             .ToArray();
        }
    }
}
