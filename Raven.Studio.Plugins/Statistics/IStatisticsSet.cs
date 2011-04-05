namespace Raven.Studio.Plugins.Statistics
{
	using Caliburn.Micro;

	public interface IStatisticsSet
	{
		IObservableCollection<IStatisticsItem> Items { get; }
	}
}