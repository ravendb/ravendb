using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	public interface IPutTrigger 
	{
		VetoResult AllowPut(string key, JObject document, JObject metadata);
		void OnPut(string key, JObject document, JObject metadata);
		void AfterCommit(string key, JObject document, JObject metadata);
	}
}