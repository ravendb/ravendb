using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IPutTrigger 
	{
		VetoResult AllowPut(string key, JObject document, JObject metadata);
		void OnPut(string key, JObject document, JObject metadata);
		void AfterCommit(string key, JObject document, JObject metadata);
	}
}