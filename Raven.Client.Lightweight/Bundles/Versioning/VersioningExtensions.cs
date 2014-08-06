
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Document.Async;

namespace Raven.Client.Bundles.Versioning
{
	public static class VersioningExtensions
	{
		public static T[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
		{
			var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)session);
			var jsonDocuments = ((DocumentSession)session).DatabaseCommands.StartsWith(id + "/revisions/", null, start, pageSize);
			return jsonDocuments
				.Select(inMemoryDocumentSessionOperations.TrackEntity<T>)
				.ToArray();
		}

		public static string[] GetRevisionIdsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
		{
			var jsonDocuments = ((DocumentSession)session).DatabaseCommands.StartsWith(id + "/revisions/", null, start, pageSize, metadataOnly: true);
			return jsonDocuments
				.Select(document => document.Key)
				.ToArray();
		}
		public static async Task<T[]> GetRevisionsForAsync<T>(this IAsyncAdvancedSessionOperations session, string id, int start, int pageSize)
		{
			var inMemoryDocumentSessionOperations = (InMemoryDocumentSessionOperations)session;
			var jsonDocuments = await ((AsyncDocumentSession)session).AsyncDatabaseCommands.StartsWithAsync(id + "/revisions/", null, start, pageSize);
			return jsonDocuments
				.Select(inMemoryDocumentSessionOperations.TrackEntity<T>)
				.ToArray();
		}

		public static async Task<string[]> GetRevisionIdsForAsync<T>(this IAsyncAdvancedSessionOperations session, string id, int start, int pageSize)
		{
			var jsonDocuments = await ((AsyncDocumentSession)session).AsyncDatabaseCommands.StartsWithAsync(id + "/revisions/", null, start, pageSize, metadataOnly: true);
			return jsonDocuments
				.Select(document => document.Key)
				.ToArray();
		}
	}
}
