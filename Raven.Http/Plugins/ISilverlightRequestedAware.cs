using System.ComponentModel.Composition;

namespace Raven.Http.Plugins
{
	[InheritedExport]
	public interface ISilverlightRequestedAware
	{
		void SilverlightWasRequested(IResourceStore resourceStore);
	}
}