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
			SeletedViewOption = new Observable<string>();
			SeletedViewOption.Value = "All";
			SeletedViewOption.PropertyChanged += (sender, args) => UpdateView();
			UpdateStatistics();
			ApplicationModel.Database.Value.Statistics.PropertyChanged +=
				(sender, args) => UpdateStatistics();
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

					if(list.First().GetType() != typeof(string) && list.First().GetType() != typeof(IndexStats))
						continue;

					var statInfo = new StatInfo
					{
						IsList = true,
						ListItems = new List<StatInfoItem>(),
					};

					statInfo.ItemsType = list.First().GetType();

					foreach (var item in list)
					{
						var statInfoItem = new StatInfoItem(item);
						var name = item.GetType().GetProperty("Name");

						if (name != null)
						{
							statInfoItem.Title = name.GetValue(item, null).ToString();
						}
						else
						{
							statInfoItem.Title = "";
						}

						statInfo.ListItems.Add(statInfoItem);
					}

					Statistics.Add(propertyInfo.Name, statInfo);
					ViewOptions.Add(propertyInfo.Name);
				}
				else
				{
					Statistics.Add(propertyInfo.Name, new StatInfo
					{
						Message = propertyInfo.GetValue(StatsData.Value, null).ToString()
					});
				}
			}

			OnPropertyChanged(() => StatsData);
			UpdateView();
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
		public Type ItemsType { get; set; }
	}

	public class StatInfoItem
	{
		public string Title { get; set; }
		public object Item { get; set; }

		public StatInfoItem(object item)
		{
			Item = item;
		}
	}
}