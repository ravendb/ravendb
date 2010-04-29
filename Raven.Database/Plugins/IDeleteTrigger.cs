using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IDeleteTrigger
	{
		VetoResult AllowDelete(string key);
		void OnDelete(string key);
		void AfterCommit(string key);
	}
}