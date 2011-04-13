namespace Raven.Studio.Features.Statistics
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Plugins.Statistics;
	using Raven.Database.Data;

	[Export]
	public class StatisticsViewModel: IStatisticsSet
	{
		readonly IEventAggregator events;
		readonly Dictionary<string, Statistic> hash = new Dictionary<string, Statistic>();

		[ImportingConstructor]
		public StatisticsViewModel(IEventAggregator events)
		{
			Items = new BindableCollection<IStatisticsItem>();
			this.events = events;
		}

		public IObservableCollection<IStatisticsItem> Items { get; private set; }

		public void Accept(DatabaseStatistics stats)
		{
			UpdateOrSetStatEntry("documents", stats.CountOfDocuments, IoC.Get<Documents.BrowseDocumentsViewModel>);
			UpdateOrSetStatEntry("indexes", stats.CountOfIndexes, IoC.Get<IndexesViewModel>);
            UpdateOrSetStatEntry("stale", stats.StaleIndexes.Length, IoC.Get<IndexesViewModel>);
			UpdateOrSetStatEntry("errors", stats.Errors.Length, IoC.Get<ErrorsViewModel>);
			UpdateOrSetStatEntry("triggers", stats.Triggers.Length, null);
			UpdateOrSetStatEntry("tasks", stats.ApproximateTaskCount, null);
		}

		public void RaiseMessageForStat(IStatisticsItem item)
		{
			if(item.ScreenToOpen == null) return;
			events.Publish(new Messages.DatabaseScreenRequested(item.ScreenToOpen));
		}

		void UpdateOrSetStatEntry(string label, object value, Func<IScreen> openScreen)
		{
			UpdateOrSetStatEntry(new Statistic
			                     	{
			                     		Label = label,
			                     		Value = value.ToString(),
										ScreenToOpen = openScreen
			                     	});
		}

		void UpdateOrSetStatEntry(Statistic entry)
		{
			var key = entry.Label;
			if (!hash.ContainsKey(key))
			{
				hash[key] = entry;
				Items.Add(entry);
			}
			else
			{
				hash[key].Value = entry.Value;
			}
		}
	}
}