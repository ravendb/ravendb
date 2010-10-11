using Newtonsoft.Json.Linq;

namespace Raven.Client
{
    /// <summary>
	/// Hook for users to provide additioanl logic on delete operations
	/// </summary>
	public interface IDocumentDeleteListener
	{
		/// <summary>
		/// Invoked before the delete request is sent to the server.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="entityInstance">The entity instance.</param>
		/// <param name="metadata">The metadata.</param>
		void BeforeDelete(string key, object entityInstance, JObject metadata);
	}
}
