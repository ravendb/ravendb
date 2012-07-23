using System.Linq;
using Raven.Client.Document;

namespace Raven.Client.Bundles.Versioning
{
	public static class VersioningExtensions
	{
		public static T[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
		{
			var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)session);
			var jsonDocuments = session.DocumentStore.DatabaseCommands.StartsWith(id + "/revisions/", null,start, pageSize);
			return jsonDocuments
				.Select(inMemoryDocumentSessionOperations.TrackEntity<T>)
				.ToArray();
		}
	}
}