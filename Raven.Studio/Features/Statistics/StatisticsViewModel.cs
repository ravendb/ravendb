namespace Raven.Studio.Features.Statistics
{
	using System.Collections.Generic;
	using Caliburn.Micro;
	using Raven.Database.Data;

	public class StatisticsViewModel: IStatisticsSet
	{
		readonly Dictionary<string, Statistic> hash = new Dictionary<string, Statistic>();
		public StatisticsViewModel() { Items = new BindableCollection<IStatisticsItem>(); }

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

		public void OpenCorrespondingScreen(IStatisticsItem item)
		{
			
		}

		void UpdateOrSetStatEntry(string label, object value)
		{
			UpdateOrSetStatEntry(new Statistic
			                     	{
			                     		Label = label,
			                     		Value = value.ToString()
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