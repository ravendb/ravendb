using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesModel : PageViewModel
	{
		public static BindableCollection<IndexListItem> GroupedIndexes { get; private set; }

		static IndexesModel()
		{
			GroupedIndexes = new BindableCollection<IndexListItem>(x => x);
		}

		public IndexesModel()
		{
			ModelUrl = "/indexes";
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands
				.GetIndexNamesAsync(0, 256)
				.ContinueOnSuccess(UpdateGroupedIndexList);
		}

		private void UpdateGroupedIndexList(IList<string> indexes)
		{
			var indexGroups = from index in indexes
							  let groupDetails = GetIndexGroup(index)
							  let indexGroup = groupDetails.Item1
							  let indexOrder = groupDetails.Item2
							  orderby indexOrder
							  group index by indexGroup;

			var indexesAndGroupHeaders = indexGroups.SelectMany(group => new IndexListItem[] { new IndexGroupHeader { Name = group.Key } }
																		.Concat(group.Select(index => new IndexItem { IndexName = index })
																		.Cast<IndexListItem>()));

			GroupedIndexes.Match(indexesAndGroupHeaders.ToList());
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