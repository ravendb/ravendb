namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel;
	using System.ComponentModel.Composition;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client;
	using Messages;
	using Plugin;

	public class BrowseIndexesViewModel : Conductor<IndexViewModel>.Collection.OneActive, IRavenScreen,
	                                      IHandle<IndexChangeMessage>
	{
		const string WatermarkFilterString = "search by index name";
		string filter;

		bool isBusy;

		public BrowseIndexesViewModel(IDatabase database)
		{
			DisplayName = "Browse Indexes";
			Database = database;

			CompositionInitializer.SatisfyImports(this);
		}

		public IDatabase Database { get; private set; }

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		[Import]
		public IWindowManager WindowManager { get; set; }

		public BindablePagedQuery<string> Indexes {get;private set;}

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

			Database.Session.Advanced.AsyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueWith(x=> RefreshIndexes(x.Result.CountOfIndexes));
		}

		void RefreshIndexes(int totalIndexCount)
		{
			Indexes = new BindablePagedQuery<string>(Database.Session.Advanced.AsyncDatabaseCommands.GetIndexNamesAsync)
			          	{GetTotalResults = () => totalIndexCount};
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

	public class BindablePagedQuery<T> : BindableCollection<T>
	{
		readonly Func<int, int, Task<T[]>> query;
		const int PageSize = 8;
		int currentPage;
		int numberOfPages;
		bool isLoading;

		public BindablePagedQuery(Func<int,int,Task<T[]>> query)
		{
			this.query = query;
		}

		public Func<int> GetTotalResults {get;set;}

		public bool HasResults
		{
			get { return NumberOfPages > 0; }
		}

		public bool HasNoResults
		{
			get { return !HasResults; }
		}

		public int CurrentPage
		{
			get { return currentPage; }
			set
			{
				currentPage = value;
				NotifyOfPropertyChange("CurrentPage");
				NotifyOfPropertyChange("CanMovePrevious");
				NotifyOfPropertyChange("CanMoveNext");
				NotifyOfPropertyChange("Status");
			}
		}

		public int NumberOfPages
		{
			get { return numberOfPages; }
			set
			{
				numberOfPages = value;
				NotifyOfPropertyChange("NumberOfPages");
				NotifyOfPropertyChange("CanMoveNext");
				NotifyOfPropertyChange("Status");
				NotifyOfPropertyChange("HasResults");
				NotifyOfPropertyChange("HasNoResults");
			}
		}

		public bool CanMovePrevious
		{
			get { return CurrentPage > 0; }
		}

		public bool CanMoveNext
		{
			get { return CurrentPage + 1 < NumberOfPages; }
		}

		public string Status
		{
			get { return string.Format("Page {0} of {1}", CurrentPage + 1, NumberOfPages); }
		}

		public bool IsLoading
		{
			get { return isLoading; }
			set
			{
				isLoading = value;
				NotifyOfPropertyChange("IsLoading");
			}
		}

		public void LoadPage(int page = 0)
		{
			IsLoading = true;
			
			query(currentPage, PageSize)
				.ContinueWith(x =>
				              	{
									Clear();
									AddRange(x.Result);
									CurrentPage = page;
				              		IsLoading = false;
									var total = GetTotalResults();
									NumberOfPages = total / PageSize + (total % PageSize == 0 ? 0 : 1);
				              	});
		}

		public void MoveNext()
		{
			LoadPage(CurrentPage + 1);
		}

		public void MovePrevious()
		{
			LoadPage(CurrentPage - 1);
		}

		protected override void ClearItems()
		{
			base.ClearItems();

			numberOfPages = 0;
			currentPage = 0;
			NotifyOfPropertyChange("NumberOfPages");
			NotifyOfPropertyChange("CurrentPage");
			NotifyOfPropertyChange("CanMoveNext");
			NotifyOfPropertyChange("Status");
			NotifyOfPropertyChange("HasResults");
			NotifyOfPropertyChange("HasNoResults");
		}
	}
}