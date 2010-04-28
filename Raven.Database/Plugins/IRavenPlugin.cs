using Lucene.Net.Documents;

namespace Raven.Database.Plugins
{
	public interface IRavenPlugin
	{
		void Initialize(DocumentDatabase database);
	}
}