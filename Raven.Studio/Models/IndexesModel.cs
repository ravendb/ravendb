using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesModel : ViewModel
	{
        public BindableCollection<IndexListItemModel> GroupedIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			GroupedIndexes = new BindableCollection<IndexListItemModel>(x => x);
			ForceTimerTicked();
		}

		protected override Task LoadedTimerTickedAsync()
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

	        var indexesAndGroupHeaders =
	            indexGroups.SelectMany(
	                group => (new IndexListItemModel[] {new IndexGroupHeader() {Name = group.Key}})
	                             .Concat(
	                                 group.Select(index => new IndexModel() {IndexName = index}).Cast<IndexListItemModel>()));

	        GroupedIndexes.Set(indexesAndGroupHeaders);
	    }

        private Tuple<string,int> GetIndexGroup(string indexName)
        {
            if (indexName.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase))
            {
                return Tuple.Create("Temp Indexes", 1);
            }
            else if (indexName.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase))
            {
                return Tuple.Create("Auto Indexes", 2);
            }
            else
            {
                return Tuple.Create("Indexes", 3);
            }
        }

	}
}