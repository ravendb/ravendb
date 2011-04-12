namespace Raven.Studio.Framework
{
	using System;
	using System.Linq;
	using System.Threading.Tasks;
	using System.Windows;
	using Caliburn.Micro;
	using Messages;
	using Action = System.Action;

	public interface IBindablePagedQuery
	{
		Size? ItemElementSize { get; set; }
		Size PageElementSize { get; set; }
		void AdjustResultsForPageSize();
	    void ClearResults();
		event EventHandler<EventArgs<bool>> IsLoadingChanged;
	}

	public class BindablePagedQuery<T> : BindablePagedQuery<T, T>
	{
		public BindablePagedQuery(Func<int, int, Task<T[]>> query)
			: base(query, t => t)
		{
		}
	}

	public class BindablePagedQuery<TResult, TViewModel> : BindableCollection<TViewModel>, IBindablePagedQuery
	{
		Func<int, int, Task<TResult[]>> query;
		readonly Func<TResult, TViewModel> transform;
		int currentPage;
		bool isLoading;
		int pageSize;
		bool hasLoadedFirstPage;

		public BindablePagedQuery(Func<int, int, Task<TResult[]>> query, Func<TResult, TViewModel> transform)
		{
			this.query = query;
			this.transform = transform;
			PageSize = 8;
			GetTotalResults = ()=> 0;
		}

		public event EventHandler<EventArgs<bool>> IsLoadingChanged = delegate { };

		public int PageSize
		{
			get { return CalculateItemsPerPage(); }
			set { pageSize = value; }
		}

		public Size? ItemElementSize { get; set; }
		public Size PageElementSize { get; set; }

        public void ClearResults()
        {
            hasLoadedFirstPage = false;
            
            Clear();

            NumberOfPages = 0;
            CurrentPage = 0;

			NotifyOfPropertyChange("HasResults");
        }

		public void AdjustResultsForPageSize()
		{
			if (!hasLoadedFirstPage) return;
			if(IsLoading) return;

			IsLoading = true;

			if (Count >= PageSize)
			{
				RemoveOverflowResults();
			}
			else
			{
				RequestAdditionalResults();
			}
			
		}
		void RemoveOverflowResults()
		{
			for (int i = PageSize + 1; i < Count; i++)
			{
				RemoveAt(i);
			}

			AdjustNumberOfPages();

			IsLoading = false;
		}

		void AdjustNumberOfPages()
		{
			var total = GetTotalResults();
			if(PageSize == 0 ) return;
			NumberOfPages = Convert.ToInt32(total / PageSize + (total % PageSize == 0 ? 0 : 1));
		}

		void RequestAdditionalResults()
		{
			var delta = PageSize - Count;
			var start = (CurrentPage * PageSize) + Count;

			query(start, delta)
				.ContinueWith(x =>
				              	{
				              		IsNotifying = false;
				              		AddRange(x.Result.Select(transform));
				              		IsNotifying = true;

									AdjustNumberOfPages();

				              		Refresh();

				              		IsLoading = false;
				              	});
		}

		int CalculateItemsPerPage()
		{	
			if(!ItemElementSize.HasValue) return pageSize;

			var itemSize = ItemElementSize.Value;

			var cols = Math.Floor(PageElementSize.Width /itemSize.Width);
			var rows = Math.Floor(PageElementSize.Height /itemSize.Height);

			return (int)(cols * rows);
		}

		public Func<long> GetTotalResults { get; set; }

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
				NotifyOfPropertyChange("CanMoveFirst");
				NotifyOfPropertyChange("CanMoveLast");
				NotifyOfPropertyChange("Status");
			}
		}

		public int NumberOfPages { get; private set; }

		public bool CanMoveFirst
		{
			get { return CurrentPage > 0; }
		}

		public bool CanMoveLast
		{
			get { return NumberOfPages > 1 && CanMoveNext; }
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
			get
			{
				return (NumberOfPages == 0)
					? "No results"
					: string.Format("Page {0} of {1}", CurrentPage + 1, NumberOfPages);
			}
		}

		public bool IsLoading
		{
			get { return isLoading; }
			set
			{
				isLoading = value;
				NotifyOfPropertyChange("IsLoading");
				IsLoadingChanged(this, new EventArgs<bool>(isLoading));
			}
		}

		public Func<int, int, Task<TResult[]>> Query
		{
			set { query = value; }
		}

		public void LoadPage(int page = 0)
		{
			LoadPage(null, page);
		}

		public void LoadPage(Action afterLoaded, int page = 0)
		{
			//HACK:
			IoC.Get<IEventAggregator>().Publish(new WorkStarted("loading page"));

			IsLoading = true;

			query(page * PageSize, PageSize)
				.ContinueWith(x =>
								{
									hasLoadedFirstPage = true;

									IsNotifying = false;
									Clear();
									AddRange(x.Result.Select(transform));
									IsNotifying = true;

									CurrentPage = page;
									AdjustNumberOfPages();

									Refresh();

									IsLoading = false;
									
									//HACK:
									IoC.Get<IEventAggregator>().Publish(new WorkCompleted("loading page"));

									if (afterLoaded != null) afterLoaded();
								});
		}

		public void MoveFirst()
		{
			LoadPage();
		}

		public void MoveLast()
		{
			LoadPage(NumberOfPages - 1);
		}

		public void MoveNext()
		{
			LoadPage(CurrentPage + 1);
		}

		public void MovePrevious()
		{
			LoadPage(CurrentPage - 1);
		}
	}
}