using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesModel : ViewModel
	{
		public BindableCollection<string> Indexes { get; private set; }
		public BindableCollection<string> TempIndexes { get; private set; }
		public BindableCollection<string> AutoIndexes { get; private set; }
		public BindableCollection<string> OtherIndexes { get; private set; }

		public IndexesModel()
		{
			ModelUrl = "/indexes";
			Indexes = new BindableCollection<string>(new PrimaryKeyComparer<string>(name => name));
			TempIndexes = new BindableCollection<string>(new PrimaryKeyComparer<string>(name => name));
			AutoIndexes = new BindableCollection<string>(new PrimaryKeyComparer<string>(name => name));
			OtherIndexes = new BindableCollection<string>(new PrimaryKeyComparer<string>(name => name));
			ForceTimerTicked();
		}

		protected override Task LoadedTimerTickedAsync()
		{
			return DatabaseCommands
				.GetIndexNamesAsync(0, 256)
				.ContinueOnSuccess(indexes =>
								   {
									   Indexes.Match(indexes);
									   TempIndexes.Match(indexes.Where(name => name.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase)).ToList());
									   AutoIndexes.Match(indexes.Where(name => name.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase)).ToList());
									   OtherIndexes.Match(indexes.Where(name => name.StartsWith("Temp/", StringComparison.InvariantCultureIgnoreCase) == false &&
										   name.StartsWith("Auto/", StringComparison.InvariantCultureIgnoreCase) == false).ToList());
								   });
		}
	}
}