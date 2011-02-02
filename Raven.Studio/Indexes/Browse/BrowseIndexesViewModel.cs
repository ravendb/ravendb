namespace Raven.Studio.Indexes.Browse
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database.Indexing;

	public class BrowseIndexesViewModel : Conductor<IndexViewModel>.Collection.OneActive,
	                                      IRavenScreen,
	                                      IHandle<IndexChangeMessage>
	{
		readonly IDatabase database;
		string filter;
		bool isBusy;

		public BrowseIndexesViewModel(IDatabase database)
		{
			DisplayName = "Browse Indexes";

			this.database = database;
			Indexes = new BindablePagedQuery<IndexDefinition>(this.database.Session.Advanced.AsyncDatabaseCommands.GetIndexesAsync);

			CompositionInitializer.SatisfyImports(this);
		}

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		public BindablePagedQuery<IndexDefinition> Indexes { get; private set; }

		IndexDefinition activeIndex;
		public IndexDefinition ActiveIndex
		{
			get { return activeIndex; }
			set { activeIndex = value; NotifyOfPropertyChange(()=>ActiveIndex); }
		}

		public string Filter
		{
			get { return filter; }
			set
			{
				if (filter != value)
				{
					filter = value;
					NotifyOfPropertyChange(() => Filter);
					Search(filter);
				}
			}
		}

		public void Handle(IndexChangeMessage message)
		{
			//IndexViewModel index = message.Index;

			//if (index.Database == Database)
			//{
			//    if (message.IsRemoved)
			//    {
			//        AllItems.Remove(index);
			//        Items.Remove(index);
			//    }
			//    else
			//    {
			//        AllItems.Add(index);
			//        Items.Add(index);
			//    }
			//}
		}

		public IRavenScreen ParentRavenScreen { get; set; }

		public SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		protected override void OnInitialize()
		{
			IsBusy = true;

			database.Session.Advanced.AsyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueWith(x => RefreshIndexes(x.Result.CountOfIndexes));
		}

		void RefreshIndexes(int totalIndexCount)
		{
			Indexes.GetTotalResults = () => totalIndexCount;
			Indexes.LoadPage();
			IsBusy = false;
		}

		public void Search(string text)
		{
			//text = text.Trim();
			//Items.Clear();

			//Items.AddRange(!string.IsNullOrEmpty(text) && text != WatermarkFilterString
			//                ? AllItems.Where(item => item.IndexOf(text, StringComparison.InvariantCultureIgnoreCase) >= 0)
			//                : AllItems);
		}
	}
}