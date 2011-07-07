using Raven.Studio.Features.Database;

namespace Raven.Studio.Shell
{
	using Caliburn.Micro;

	public interface IShell : IConductor, IHaveActiveItem
	{
		NavigationViewModel Navigation { get; }
		SelectDatabaseViewModel StartScreen { get; }
		DatabaseExplorer DatabaseScreen { get; }
	}
}