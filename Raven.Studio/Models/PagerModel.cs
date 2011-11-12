using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class PagerModel : NotifyPropertyChangedBase
	{
		public PagerModel()
		{
			PageSize = 25;
			SetTotalResults(new Observable<long>());
		}

		public void SetTotalResults(Observable<long> observable)
		{
			TotalResults = observable;
			TotalResults.PropertyChanged += (sender, args) =>
			                                {
			                                	OnPropertyChanged("TotalPages");
			                                	OnPropertyChanged("HasNextPage");
			                                };
		}

		public int PageSize { get; set; }

		public int CurrentPage
		{
			get { return Skip / PageSize + 1; }
		}

		public Observable<long> TotalResults { get; private set; }
		public long TotalPages
		{
			get { return TotalResults.Value / PageSize + 1; }
		}

		private ushort? skip;
		public ushort Skip
		{
			get
			{
				if (skip == null)
				{
					ushort s;
					ushort.TryParse(new UrlParser(UrlUtil.Url).GetQueryParam("skip"), out s);
					skip = s;
				}
				return skip.Value;
			}
		}

		public bool HasNextPage()
		{
			return CurrentPage < TotalPages;
		}

		public bool HasPrevPage()
		{
			return CurrentPage > 0;
		}

		public bool NavigateToNextPage()
		{
			if (HasNextPage() == false)
				return false;
			NavigateToPage(1);
			return true;
		}

		public bool NavigateToPrevPage()
		{
			if (HasPrevPage() == false)
				return false;
			NavigateToPage(-1);
			return true;
		}

		private void NavigateToPage(int pageOffset)
		{
			var urlParser = new UrlParser(UrlUtil.Url);
			urlParser.SetQueryParam("skip", skip + pageOffset * PageSize);
			UrlUtil.Navigate(urlParser.BuildUrl());
		}

		public ICommand NextPage
		{
			get { return new NavigateToNextPageCommand(this); }
		}

		public ICommand PrevPage
		{
			get { return new NavigateToPrevPageCommand(this); }
		}
	}
}