using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class IndexesModel : PageViewModel
	{
		private ICommand deleteIndex;
		private ICommand resetIndex;
		public ObservableCollection<IndexListItem> GroupedIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
																	   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
																	   "/indexes";
			GroupedIndexes =
				new ObservableCollection<IndexListItem>();
			ItemSelection = new ItemSelection<IndexItem>();
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccessInTheUIThread(UpdateGroupedIndexList);
		}

		public ItemSelection<IndexItem> ItemSelection { get; private set; }

		public ICommand DeleteIndex { get { return deleteIndex ?? (deleteIndex = new DeleteIndexCommand(this)); } }
		public ICommand ResetIndex { get { return resetIndex ?? (resetIndex = new ResetIndexCommand(this)); } }
		public ICommand DeleteIndexes { get { return new DeleteIndexesCommand(this); } }

		private void UpdateGroupedIndexList(DatabaseStatistics statistics)
		{
			var indexes = statistics.Indexes;
			var currentSelection = ItemSelection.GetSelectedItems().Select(i => i.Name).ToHashSet();

			var indexGroups = from index in indexes
							  let groupDetails = GetIndexGroup(index.Name)
							  let indexGroup = groupDetails.Item1
							  let indexOrder = groupDetails.Item2
							  orderby indexOrder
							  group index by indexGroup;

			var indexesAndGroupHeaders =
				indexGroups.SelectMany(group => new IndexListItem[] {new IndexGroupHeader {Name = group.Key}}
													.Concat(group.Select(index => new IndexItem {Name = index.Name, IndexStats = index})));

			GroupedIndexes.Clear();
			GroupedIndexes.AddRange(indexesAndGroupHeaders.ToList());

			var selection = GroupedIndexes.OfType<IndexItem>().Where(i => currentSelection.Contains(i.Name));

			ItemSelection.SetDesiredSelection(selection);
		}

		private Tuple<string, int> GetIndexGroup(string indexName)
		{
			if (indexName.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
				return Tuple.Create("Auto Indexes", 2);
			return Tuple.Create("Indexes", 3);
		}

		public List<IndexListItem> IndexesOfPriority(string deleteItems)
		{
			if (deleteItems == "All")
				return GroupedIndexes.ToList();
			if (deleteItems == "Idle")
				return
					GroupedIndexes.Where(
						item => item is IndexItem && ((IndexItem) item).IndexStats.Priority.HasFlag(IndexingPriority.Idle)).ToList();
			if (deleteItems == "Disabled")
				return GroupedIndexes.Where(item => item is IndexItem && ((IndexItem)item).IndexStats.Priority.HasFlag(IndexingPriority.Disabled)).ToList();
			if (deleteItems == "Abandoned")
				return GroupedIndexes.Where(item => item is IndexItem && ((IndexItem)item).IndexStats.Priority.HasFlag(IndexingPriority.Abandoned)).ToList();

			return null;
		}
	}
}