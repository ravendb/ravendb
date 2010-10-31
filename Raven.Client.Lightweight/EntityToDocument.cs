using Newtonsoft.Json.Linq;

namespace Raven.Client
{
	/// <summary>
	/// Delegate definition for converting an entity to a document and metadata
	/// </summary>
	public delegate void EntityToDocument(object entity, JObject document, JObject metadata);
}
