namespace Raven.Studio.Framework
{
	using System;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;


	public class BindablePagedQuery<T> : BindablePagedQuery<T, T>
	{
		public BindablePagedQuery(Func<int, int, Task<T[]>> query) : base(query, t => t)
		{
		}
	}

	public class BindablePagedQuery<TResult, TViewModel> : BindableCollection<TViewModel>
	{
		public int PageSize = 8;
		readonly Func<int, int, Task<TResult[]>> query;
		readonly Func<TResult, TViewModel> transform;
		int currentPage;
		bool isLoading;

		public BindablePagedQuery(Func<int, int, Task<TResult[]>> query, Func<TResult,TViewModel> transform)
		{
			this.query = query;
			this.transform = transform;
		}

		public Func<long > GetTotalResults { get; set; }

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

		public int NumberOfPages { get; private set; }

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

			query(page * PageSize, PageSize)
				.ContinueWith(x =>
				              	{
									IsNotifying = false;
									Clear();
									AddRange(x.Result.Select(transform));
									IsNotifying = true;

				              		CurrentPage = page;
				              		var total = GetTotalResults();
				              		NumberOfPages = Convert.ToInt32( total/PageSize + (total%PageSize == 0 ? 0 : 1) );

									Refresh();

									IsLoading = false;
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
	}
}