using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IStartupTask
	{
		void Execute(DocumentDatabase database);
	}
}
