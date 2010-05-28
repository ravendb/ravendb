using Newtonsoft.Json.Linq;

namespace Raven.Client
{
	public delegate void EntityToDocument(object entity, JObject document, JObject metadata);
}