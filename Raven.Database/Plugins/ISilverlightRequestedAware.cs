using System.ComponentModel.Composition;
using Raven.Database;

namespace Raven.Http.Plugins
{
	[InheritedExport]
	public interface ISilverlightRequestedAware
	{
		void SilverlightWasRequested(DocumentDatabase database);
	}
}