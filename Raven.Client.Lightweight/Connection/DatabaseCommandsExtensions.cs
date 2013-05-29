#if !SILVERLIGHT
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public static class DatabaseCommandsExtensions
	{
		/// <summary>
		/// Sends a patch request for a specific document or, if it does not exist, puts the specified document instead, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		public static RavenJObject PatchOrPut(this IDatabaseCommands commands, string key, PatchRequest[] patches,
											  RavenJObject document, RavenJObject metadata)
		{
			return commands.PatchOrPut(key, patches, document, metadata, null);
		}
	}
}
#endif