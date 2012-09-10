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
			Statistics = new Dictionary<string, StatInfo>();
			StatisticsToView = new Dictionary<string, StatInfo>();
			ViewOptions = new List<string>();
			SeletedViewOption = new Observable<string> { Value = "All" };
			SeletedViewOption.PropertyChanged += (sender, args) => UpdateView();
			UpdateStatistics();
			ApplicationModel.Database.Value.Statistics.PropertyChanged +=
				(sender, args) => UpdateStatistics();

		}

		protected override void OnViewLoaded()
		{
			var indexToShow = new UrlParser(UrlUtil.Url).GetQueryParam("index");
			if (indexToShow != null)
				SeletedViewOption.Value = indexToShow;
			
			base.OnViewLoaded();
		}

		private void UpdateView()
		{
			StatisticsToView = new Dictionary<string, StatInfo>();
			switch (SeletedViewOption.Value)
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
						var items = Statistics.Where(pair => pair.Key == SeletedViewOption.Value).ToList();
						foreach (var item in items)
						{
							StatisticsToView.Add(item.Key, item.Value);
						}

						if (StatisticsToView.Count == 0)
						{
							var indexs = Statistics.FirstOrDefault(pair => pair.Key == "Indexes");
							var index = indexs.Value.ListItems.FirstOrDefault(item => item.Title == SeletedViewOption.Value);

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
					if (list.Count == 0)
						continue;

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
						{
							AddIndexStat(statInfoItem);
						}

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
						Message = GetValueWithFormat(propertyInfo.GetValue(StatsData.Value, null))
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

					var performanceMessage = "";

					foreach (var indexingPerformanceStatse in performance)
					{
						performanceMessage += string.Format(@"
Operation:         {0}
Count:              {1}
Duration:          {2}
Duration in ms: {3}", indexingPerformanceStatse.Operation,
															indexingPerformanceStatse.Count, indexingPerformanceStatse.Duration,
															indexingPerformanceStatse.DurationMilliseconds.ToString("#,#"));
					}

					statInfoItem.ItemData.Add("Performance", performanceMessage);

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

		public Observable<DatabaseStatistics> StatsData { get; set; }
		public Dictionary<string, StatInfo> Statistics { get; set; }
		public Dictionary<string, StatInfo> StatisticsToView { get; set; }
		public List<string> ViewOptions { get; set; }
		public Observable<string> SeletedViewOption { get; set; }
	}

	public class StatInfo
	{
		public bool IsList { get; set; }
		public string Message { get; set; }
		public List<StatInfoItem> ListItems { get; set; }
	}

	public class StatInfoItem
	{
		public object Item { get; private set; }
		public Type ItemType { get; private set; }
		public string Title { get; set; }
		public Dictionary<string, string> ItemData { get; private set; }

		public StatInfoItem(object item)
		{
			Item = item;
			ItemType = item.GetType();
			ItemData = new Dictionary<string, string>();
		}
	}
}