using Lucene.Net.Documents;

namespace Raven.Database.Plugins
{
	public interface IRequiresDocumentDatabaseInitialization
	{
		void Initialize(DocumentDatabase database);
	}
}