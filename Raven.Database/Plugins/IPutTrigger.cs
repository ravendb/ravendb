using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	public interface IPutTrigger : IRavenPlugin
	{
		VetoResult AllowPut(string key, JObject document, JObject metadata);
		void OnPut(string key, JObject document, JObject metadata);
		void AfterPut(string key, JObject document, JObject metadata);
	}
}