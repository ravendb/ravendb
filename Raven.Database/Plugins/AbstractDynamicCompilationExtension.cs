using System.ComponentModel.Composition;
using System.Reflection;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractDynamicCompilationExtension
	{
		public abstract string[] GetNamespacesToImport();
		public abstract string[] GetAssembliesToReference();
	}
}
