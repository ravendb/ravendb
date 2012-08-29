using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class IndexesModel : PageViewModel
	{
		private ICommand promoteIndex;
		private ICommand deleteIndex;
		private ICommand resetIndex;
		public ObservableCollection<IndexListItem> GroupedIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			GroupedIndexes =
				new ObservableCollection<IndexListItem>();
			ItemSelection = new ItemSelection<IndexItem>();
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands
				.GetIndexNamesAsync(0, 256)
				.ContinueOnSuccessInTheUIThread(UpdateGroupedIndexList);
		}

		public ItemSelection<IndexItem> ItemSelection { get; private set; }

		public ICommand PromoteIndex { get { return promoteIndex ?? (promoteIndex = new PromoteToAutoIndexCommand(this)); } }
		public ICommand DeleteIndex { get { return deleteIndex ?? (deleteIndex = new DeleteIndexCommand(this)); } }
		public ICommand ResetIndex { get { return resetIndex ?? (resetIndex = new ResetIndexCommand(this)); } }
		public ICommand DeleteAllIndexes { get { return new DeleteAllIndexsCommand(this); } }

		private void UpdateGroupedIndexList(IList<string> indexes)
		{
			var currentSelection = ItemSelection.GetSelectedItems().Select(i => i.Name).ToHashSet();

			var indexGroups = from index in indexes
							  let groupDetails = GetIndexGroup(index)
							  let indexGroup = groupDetails.Item1
							  let indexOrder = groupDetails.Item2
							  orderby indexOrder
							  group index by indexGroup;

			var indexesAndGroupHeaders =
				indexGroups.SelectMany(group => new IndexListItem[] {new IndexGroupHeader {Name = group.Key}}
													.Concat(group.Select(index => new IndexItem {Name = index})));

			GroupedIndexes.Clear();
			GroupedIndexes.AddRange(indexesAndGroupHeaders.ToList());

			var selection = GroupedIndexes.OfType<IndexItem>().Where(i => currentSelection.Contains(i.Name));

			ItemSelection.SetDesiredSelection(selection);
		}

		private Tuple<string, int> GetIndexGroup(string indexName)
		{
			if (indexName.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase))
				return Tuple.Create("Temp Indexes", 1);
			if (indexName.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase))
				return Tuple.Create("Auto Indexes", 2);
			return Tuple.Create("Indexes", 3);
		}
	}
}