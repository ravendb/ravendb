using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;
using IndexStats = Raven.Abstractions.Data.IndexStats;

namespace Raven.Studio.Models
{
	public class IndexesModel : PageViewModel
	{
		private ICommand deleteIndex;
		private ICommand resetIndex;
		private IndexItem itemSelection;
		private IndexGroup selectedGroup;
		public ObservableCollection<IndexItem> Indexes { get; private set; }
		public ObservableCollection<IndexGroup> GroupedIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/indexes";
			Indexes = new ObservableCollection<IndexItem>();
			GroupedIndexes = new ObservableCollection<IndexGroup>();
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccessInTheUIThread(UpdateGroupedIndexList);
		}

		public IndexItem ItemSelection
		{
			get { return itemSelection; }
			set
			{
				if(value == null)
					return;
				itemSelection = value;
			}
		}

		public ICommand DeleteIndex { get { return deleteIndex ?? (deleteIndex = new DeleteIndexCommand(this)); } }
		public ICommand ResetIndex { get { return resetIndex ?? (resetIndex = new ResetIndexCommand(this)); } }
		public ICommand DeleteIndexes { get { return new DeleteIndexesCommand(this); } }

		public ICommand DeleteGroupIndexes
		{
			get { return new DeleteIndexesCommand(this);}
		}

		public IndexGroup SelectedGroup
		{
			get { return selectedGroup; }
			set
			{
				if (value == null)
					return;
				selectedGroup = value;
			}
		}

		public ICommand CollapseAll
		{
			get { return new ActionCommand(() =>
			{
				foreach (var groupedIndex in GroupedIndexes)
				{
					groupedIndex.Collapse.Value = true;
				}
			});}
		}

		public ICommand ExpandAll
		{
			get
			{
				return new ActionCommand(() =>
				{
					foreach (var groupedIndex in GroupedIndexes)
					{
						groupedIndex.Collapse.Value = false;
					}
				});
			}
		}

		private void UpdateGroupedIndexList(DatabaseStatistics statistics)
		{
			Indexes.Clear();
			Indexes.AddRange(statistics.Indexes.Select(stats => new IndexItem{Name = stats.Name, GroupName = GetIndexGroup(stats), IndexStats = stats}));
			//var currentSelection = ItemSelection.GetSelectedItems().Select(i => i.Name).ToHashSet();

			CleanGroupIndexes();
			foreach (var indexItem in Indexes)
			{
				var groupItem = GroupedIndexes.FirstOrDefault(@group => group.GroupName == indexItem.GroupName);
				if (groupItem == null)
				{
					groupItem = new IndexGroup(indexItem.GroupName);
					GroupedIndexes.Add(groupItem);
				}

				groupItem.Indexes.Add(indexItem);
			}

			GroupedIndexes = new ObservableCollection<IndexGroup>(GroupedIndexes.OrderBy(@group => group.GroupName));

		//	var selection = GroupedIndexes.OfType<IndexItem>().Where(i => currentSelection.Contains(i.Name));

			//ItemSelection.SetDesiredSelection(selection);
			OnPropertyChanged(() => GroupedIndexes);
		}

		private void CleanGroupIndexes()
		{
			foreach (var groupedIndex in GroupedIndexes)
			{
				groupedIndex.Indexes.Clear();
			}
		}

		private string GetIndexGroup(IndexStats index)
		{
			if (index.ForEntityName.Count == 1)
				return index.ForEntityName.First();
			return "Others";
		}

		public List<IndexItem> IndexesOfPriority(string deleteItems)
		{
			if (deleteItems == "All")
				return Indexes.ToList();
			if (deleteItems == "Idle")
				return
					Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Idle)).ToList();
			if (deleteItems == "Disabled")
				return Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Disabled)).ToList();
			if (deleteItems == "Abandoned")
				return Indexes.Where(item => item.IndexStats.Priority.HasFlag(IndexingPriority.Abandoned)).ToList();

			return null;
		}
	}
}