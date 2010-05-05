using System.ComponentModel.Composition;

namespace Raven.Database
{
	[InheritedExport]
	public interface IStartupTask
	{
		void Execute(DocumentDatabase database);
	}
}