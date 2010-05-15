using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
    [InheritedExport]
    internal interface IRequiresDocumentDatabaseInitialization
	{
		void Initialize(DocumentDatabase database);
	}
}