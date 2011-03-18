namespace Raven.Studio.Features.Statistics
{
	using System.Collections.Generic;
	using Caliburn.Micro;
	using Database;
	using Raven.Database.Data;

	public class Statistics: IStatisticsSet
	{
		readonly Dictionary<string, StatisitcsEntry> hash = new Dictionary<string, StatisitcsEntry>();
		public Statistics() { Items = new BindableCollection<IStatisticsItem>(); }

		public IObservableCollection<IStatisticsItem> Items { get; private set; }

		public void Accept(DatabaseStatistics stats)
		{
			UpdateOrSetStatEntry("documents", stats.CountOfDocuments);
			UpdateOrSetStatEntry("indexes", stats.CountOfIndexes);
			UpdateOrSetStatEntry("stale", stats.StaleIndexes.Length);
			UpdateOrSetStatEntry("errors", stats.Errors.Length);
			UpdateOrSetStatEntry("triggers", stats.Triggers.Length);
			UpdateOrSetStatEntry("tasks", stats.ApproximateTaskCount);
		}

		void UpdateOrSetStatEntry(string label, object value)
		{
			UpdateOrSetStatEntry(new StatisitcsEntry
			                     	{
			                     		Label = label,
			                     		Value = value.ToString()
			                     	});
		}

		void UpdateOrSetStatEntry(StatisitcsEntry entry)
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

	public class StatisitcsEntry : PropertyChangedBase, IStatisticsItem
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