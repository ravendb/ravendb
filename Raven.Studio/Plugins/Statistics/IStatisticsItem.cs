namespace Raven.Studio.Plugins.Statistics
{
	using System;
	using Caliburn.Micro;

	public interface IStatisticsItem
	{
		string Label { get; }
		string Value { get; }
		Func<IScreen> ScreenToOpen { get; }
	}
}