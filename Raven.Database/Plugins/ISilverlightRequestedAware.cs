using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface ISilverlightRequestedAware
	{
		void SilverlightWasRequested(DocumentDatabase database);
	}
}