#if !SILVERLIGHT && !NETFX_CORE
using System.Linq;
using Raven.Client.Document;

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
	}
}
#endif