
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Json.Linq;

namespace Raven.Client.Bundles.Versioning
{
    public static class VersioningExtensions
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static RavenJObject[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
        {
            var jsonDocuments = ((DocumentSession)session).DatabaseCommands.GetRevisionsFor(id + "/revisions/", start, pageSize);
            return jsonDocuments.ToArray();
        }

        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        public static async Task<RavenJObject[]> GetRevisionsForAsync<T>(this IAsyncAdvancedSessionOperations session, string id, int start, int pageSize)
        {
            var jsonDocuments = await ((AsyncDocumentSession)session).AsyncDatabaseCommands.GetRevisionsForAsync(id + "/revisions/", start, pageSize).ConfigureAwait(false);
            return jsonDocuments.ToArray();
        }
    }
}
