// -----------------------------------------------------------------------
//  <copyright file="StatisticsModel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class StatisticsModel : PageViewModel
	{
		public StatisticsModel()
		{
			ModelUrl = "/statistics";
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/stats";
			IndexesGraphData = new Dictionary<string, PerformanceStats>();
		    Breadcrumb = "Documents";
			Statistics = new Dictionary<string, StatInfo>();
			StatisticsToView = new Dictionary<string, StatInfo>();
			ViewOptions = new List<string>();
			SelectedViewOption = new Observable<string> { Value = "All" };
			SelectedViewOption.PropertyChanged += (sender, args) => UpdateView();
			UpdateStatistics();
			ApplicationModel.Database.Value.Statistics.PropertyChanged +=
				(sender, args) => UpdateStatistics();

		}

		protected override void OnViewLoaded()
		{
			var indexToShow = new UrlParser(UrlUtil.Url).GetQueryParam("index");
            if (indexToShow != null)
            {
                SelectedViewOption.Value = indexToShow;
                Breadcrumb = "Indexes";
                OnPropertyChanged(() => Breadcrumb);
            }

		    base.OnViewLoaded();
		}

		private void UpdateView()
		{
			StatisticsToView = new Dictionary<string, StatInfo>();
			switch (SelectedViewOption.Value)
			{
				case "All":
					foreach (var statistic in Statistics)
					{
						StatisticsToView.Add(statistic.Key, statistic.Value);
					}
					break;
				case "Single Items":
					{
						var items = Statistics.Where(pair => pair.Value.IsList == false).ToList();
						foreach (var item in items)
						{
							StatisticsToView.Add(item.Key, item.Value);
						}
					}
					break;
				default:
					{
						var items = Statistics.Where(pair => pair.Key == SelectedViewOption.Value).ToList();
						foreach (var item in items)
						{
							StatisticsToView.Add(item.Key, item.Value);
						}

						if (StatisticsToView.Count == 0)
						{
							var indexes = Statistics.FirstOrDefault(pair => pair.Key == "Indexes");
							var index = indexes.Value.ListItems.FirstOrDefault(item => item.Title == SelectedViewOption.Value);

							if (index == null)
								break;

							StatisticsToView.Add("Indexes", new StatInfo
							{
								IsList = true,
								ListItems = new List<StatInfoItem>
								{
									index
								}
							});
						}
					}
					break;
			}

			OnPropertyChanged(() => StatisticsToView);
		}

		private void UpdateStatistics()
		{
			StatsData = ApplicationModel.Database.Value.Statistics;
			Statistics.Clear();
			ViewOptions.Clear();
			ViewOptions.Add("All");
			ViewOptions.Add("Single Items");

			if (StatsData.Value == null)
			{
				Thread.Sleep(100);
				UpdateStatistics();
				return;
			}

			foreach (var propertyInfo in StatsData.Value.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
			{
				var enumerable = propertyInfo.GetValue(StatsData.Value, null) as IEnumerable<object>;

				if (enumerable != null)
				{
					var list = enumerable as List<object> ?? enumerable.ToList();

					if (propertyInfo.Name == "StaleIndexes")
					{
						if (list.Count == 0)
						{
							Statistics.Add(propertyInfo.Name, new StatInfo
							{
								Message = "No Stale Indexes",
								ToolTipData = "No Stale Indexes"
							});
						}
						else
						{
							Statistics.Add(propertyInfo.Name, new StatInfo
							{
								Message = string.Format("There are {0} Stale indexes", list.Count),
								ToolTipData = string.Join(", ", list)
							});
						}

						continue;
					}

					if (list.Count == 0)
					{
						continue;						
					}

					if ((list.First() is string == false) && (list.First() is IndexStats == false))
						continue;

					var statInfo = new StatInfo
					{
						IsList = true,
						ListItems = new List<StatInfoItem>(),
					};

					foreach (var item in list)
					{
						var statInfoItem = new StatInfoItem(item);

						if (statInfoItem.ItemType == typeof(IndexStats))
							AddIndexStat(statInfoItem);

						statInfo.ListItems.Add(statInfoItem);
					}

					Statistics.Add(propertyInfo.Name, statInfo);
					ViewOptions.Add(propertyInfo.Name);
				}
				else
				{
					if (string.IsNullOrEmpty(propertyInfo.GetValue(StatsData.Value, null).ToString()) || propertyInfo.GetValue(StatsData.Value, null).ToString() == "0")
						continue;

					Statistics.Add(propertyInfo.Name, new StatInfo
					{
						Message = GetValueWithFormat(propertyInfo.GetValue(StatsData.Value, null)),
						ToolTipData = GetValueWithFormat(propertyInfo.GetValue(StatsData.Value, null))
					});
				}
			}

			OnPropertyChanged(() => StatsData);
			UpdateView();
		}

		private string GetValueWithFormat(object value)
		{
			if (value == null)
				return null;

			if (value is int)
				return ((int)value).ToString("#,#");
			if (value is double)
				return ((double)value).ToString("#,#");
			if (value is long)
				return ((long)value).ToString("#,#");
			if (value is float)
				return ((float)value).ToString("#,#");

			return value.ToString();
		}

		private void AddIndexStat(StatInfoItem statInfoItem)
		{
			foreach (var propertyInfo in statInfoItem.Item.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
			{
				if (propertyInfo.Name == "Name")
				{
					statInfoItem.Title = propertyInfo.GetValue(statInfoItem.Item, null).ToString();
					ViewOptions.Add(statInfoItem.Title);
					continue;
				}

				if (propertyInfo.Name == "Performance")
				{
					var performance = propertyInfo.GetValue(statInfoItem.Item, null) as IndexingPerformanceStats[];
					if (performance == null || performance.Length == 0)
						continue;

					UpdateGraphDate(performance, statInfoItem);

					continue;
				}

				if (propertyInfo.GetValue(statInfoItem.Item, null) == null)
					continue;

				int isZero;
				var isInt = int.TryParse(propertyInfo.GetValue(statInfoItem.Item, null).ToString(), out isZero);
				if (isInt && isZero == 0)
					continue;

				statInfoItem.ItemData.Add(propertyInfo.Name, GetValueWithFormat(propertyInfo.GetValue(statInfoItem.Item, null)));
			}
		}

		private void UpdateGraphDate(IEnumerable<IndexingPerformanceStats> performance, StatInfoItem statInfoItem)
		{
			if(IndexesGraphData.ContainsKey( statInfoItem.Title) == false)
				IndexesGraphData.Add(statInfoItem.Title, new PerformanceStats());

			foreach (var indexingPerformanceStat in performance)
			{
				switch (indexingPerformanceStat.Operation)
				{
					case "Index":
						if (IndexesGraphData[statInfoItem.Title].IndexData.Contains(indexingPerformanceStat) == false)
							IndexesGraphData[statInfoItem.Title].IndexData.Add(indexingPerformanceStat);
						break;
					case "Map":
						if (IndexesGraphData[statInfoItem.Title].MapData.Contains(indexingPerformanceStat) == false)
							IndexesGraphData[statInfoItem.Title].MapData.Add(indexingPerformanceStat);
						break;
					case "Reduce Level 0":
						if (IndexesGraphData[statInfoItem.Title].Level0Data.Contains(indexingPerformanceStat) == false)
							IndexesGraphData[statInfoItem.Title].Level0Data.Add(indexingPerformanceStat);
						break;
					case "Reduce Level 1":
						if (IndexesGraphData[statInfoItem.Title].Level1Data.Contains(indexingPerformanceStat) == false)
							IndexesGraphData[statInfoItem.Title].Level1Data.Add(indexingPerformanceStat);
						break;
					case "Reduce Level 2":
						if (IndexesGraphData[statInfoItem.Title].Level2Data.Contains(indexingPerformanceStat) == false)
							IndexesGraphData[statInfoItem.Title].Level2Data.Add(indexingPerformanceStat);
						break;
				}
			}

			UpdateStatInfoFromGraphData(statInfoItem);
		}

		private void UpdateStatInfoFromGraphData(StatInfoItem statInfoItem)
		{
			foreach (var indexingPerformanceStats in IndexesGraphData[statInfoItem.Title].IndexData)
			{
				statInfoItem.IndexData.Add(indexingPerformanceStats.Started, indexingPerformanceStats);
			}

			foreach (var indexingPerformanceStats in IndexesGraphData[statInfoItem.Title].MapData)
			{
				statInfoItem.MapData.Add(indexingPerformanceStats.Started, indexingPerformanceStats);
			}

			foreach (var indexingPerformanceStats in IndexesGraphData[statInfoItem.Title].Level0Data)
			{
				statInfoItem.Level0Data.Add(indexingPerformanceStats.Started, indexingPerformanceStats);
			}

			foreach (var indexingPerformanceStats in IndexesGraphData[statInfoItem.Title].Level1Data)
			{
				statInfoItem.Level1Data.Add(indexingPerformanceStats.Started, indexingPerformanceStats);
			}

			foreach (var indexingPerformanceStats in IndexesGraphData[statInfoItem.Title].Level2Data)
			{
				statInfoItem.Level2Data.Add(indexingPerformanceStats.Started, indexingPerformanceStats);
			}
		}

		public Observable<DatabaseStatistics> StatsData { get; set; }
		public Dictionary<string, StatInfo> Statistics { get; set; }
		public Dictionary<string, StatInfo> StatisticsToView { get; set; }
		public List<string> ViewOptions { get; set; }
		public Dictionary<string, PerformanceStats> IndexesGraphData { get; set; } 
		public Observable<string> SelectedViewOption { get; set; }
        public string Breadcrumb { get; set; }
	}

	public class PerformanceStats
	{
		public List<IndexingPerformanceStats> MapData { get; set; }
		public List<IndexingPerformanceStats> IndexData { get; set; }
		public List<IndexingPerformanceStats> Level0Data { get; set; }
		public List<IndexingPerformanceStats> Level1Data { get; set; }
		public List<IndexingPerformanceStats> Level2Data { get; set; }

		public PerformanceStats()
		{
			MapData = new List<IndexingPerformanceStats>();
			IndexData = new List<IndexingPerformanceStats>();
			Level0Data = new List<IndexingPerformanceStats>();
			Level1Data = new List<IndexingPerformanceStats>();
			Level2Data = new List<IndexingPerformanceStats>();
		}
	}

	public class StatInfo
	{
		public bool IsList { get; set; }
		public string Message { get; set; }
		public string ToolTipData { get; set; }
		public List<StatInfoItem> ListItems { get; set; }
	}

	public class StatInfoItem
	{
		public object Item { get; private set; }
		public Type ItemType { get; private set; }
		public string Title { get; set; }
		public Dictionary<string, string> ItemData { get; private set; }
		public Dictionary<DateTime, IndexingPerformanceStats> MapData { get; set; }
		public Dictionary<DateTime, IndexingPerformanceStats> IndexData { get; set; }
		public Dictionary<DateTime, IndexingPerformanceStats> Level0Data { get; set; }
		public Dictionary<DateTime, IndexingPerformanceStats> Level1Data { get; set; }
		public Dictionary<DateTime, IndexingPerformanceStats> Level2Data { get; set; }

		public bool ShowChart { get { return MapData.Count != 0  || Level0Data.Count != 0|| Level1Data.Count != 0 || Level2Data.Count != 0 || IndexData.Count != 0; } }

		public StatInfoItem(object item)
		{
			Item = item;
			ItemType = item.GetType();
			ItemData = new Dictionary<string, string>();
			MapData = new Dictionary<DateTime, IndexingPerformanceStats>();
			Level0Data = new Dictionary<DateTime, IndexingPerformanceStats>();
			Level1Data = new Dictionary<DateTime, IndexingPerformanceStats>();
			Level2Data = new Dictionary<DateTime, IndexingPerformanceStats>();
			IndexData = new Dictionary<DateTime, IndexingPerformanceStats>();
		}
	}
}