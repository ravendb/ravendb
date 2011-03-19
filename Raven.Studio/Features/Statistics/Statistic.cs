namespace Raven.Studio.Features.Statistics
{
	using Caliburn.Micro;

	public class Statistic : PropertyChangedBase, IStatisticsItem
	{
		string value;
		public string Label { get; set; }

		public string Value
		{
			get { return value; }
			set
			{
				this.value = value;
				NotifyOfPropertyChange(() => Value);
			}
		}
	}
}