using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	public interface IDeleteTrigger : IRavenPlugin
	{
		VetoResult AllowDelete(string key);
		void OnDelete(string key);
		void AfterDelete(string key);
	}
}