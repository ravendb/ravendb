using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
    [InheritedExport]
	public interface IRequiresDocumentDatabaseInitialization
	{
		void Initialize(DocumentDatabase database);
	}
}