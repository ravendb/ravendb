using System.Linq;
using Raven.Client.Document;

namespace Raven.Client.Versioning
{
	public static class VersioningClientExtensions
	{
		public static T[] GetRevisionsFor<T>(this ISyncAdvancedSessionOperation session, string id, int start, int pageSize)
		{
			var inMemoryDocumentSessionOperations = ((InMemoryDocumentSessionOperations)session);
			var jsonDocuments = session.DatabaseCommands.StartsWith(id + "/revisions/", start, pageSize);
			return jsonDocuments
				.Select(jsonDocument => inMemoryDocumentSessionOperations.TrackEntity<T>(jsonDocument.Key, jsonDocument.DataAsJson, jsonDocument.Metadata))
				.ToArray();
		}
	}
}
