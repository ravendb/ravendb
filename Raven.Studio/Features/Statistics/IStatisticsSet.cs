namespace Raven.Studio.Features.Statistics
{
	using Caliburn.Micro;

	public interface IStatisticsSet
	{
		IObservableCollection<IStatisticsItem> Items { get; }
	}
}