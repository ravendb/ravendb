namespace Raven.Studio.Features.Statistics
{
	using System;
	using Caliburn.Micro;
	using Messages;

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

		public Func<IScreen> ScreenToOpen { get; set; }
	}
}