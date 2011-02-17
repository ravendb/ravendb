namespace Raven.Studio.Features.Indexes
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database.Indexing;

	[Export]
	public class BrowseIndexesViewModel : Conductor<EditIndexViewModel>,
	                                      IHandle<IndexUpdated>
	{
		readonly IServer server;
		IndexDefinition activeIndex;
		string filter;
		bool isBusy;

		[ImportingConstructor]
		public BrowseIndexesViewModel(IServer server)
		{
			DisplayName = "Indexes";

			this.server = server;
		}

		protected override void OnActivate()
		{
			var session = server.OpenSession();
			Indexes = new BindablePagedQuery<IndexDefinition>(session.Advanced.AsyncDatabaseCommands.GetIndexesAsync);
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

		public IndexDefinition ActiveIndex
		{
			get { return activeIndex; }
			set
			{
				activeIndex = value;
				if (activeIndex != null)
					ActiveItem = new EditIndexViewModel(activeIndex, server);
				NotifyOfPropertyChange(() => ActiveIndex);
			}
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

		public void Handle(IndexUpdated message)
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

		protected override void OnInitialize()
		{
			IsBusy = true;

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x => RefreshIndexes(x.Result.CountOfIndexes));
			}
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