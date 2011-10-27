using System;
using System.Windows.Browser;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class PagerModel : NotifyPropertyChangedBase
	{
		private readonly UrlUtil url;

		public PagerModel()
		{
			PageSize = 25;
			TotalPages = new Observable<long>();
			url = new UrlUtil();
		}

		public int PageSize { get; set; }

		public int CurrentPage
		{
			get { return Skip / PageSize; }
		}

		public Observable<long> TotalPages { get; set; }

		private ushort skip;
		public ushort Skip
		{
			get
			{
				if (skip == 0)
				{
					ushort.TryParse(url.GetQueryParam("skip"), out skip);
					if (skip < 1)
						skip = 1;
				}
				return skip;
			}
		}

		public bool HasNextPage()
		{
			return CurrentPage < TotalPages.Value;
		}

		public bool HasPrevPage()
		{
			return CurrentPage > 0;
		}

		public bool NavigateToNextPage()
		{
			if (HasNextPage() == false)
				return false;
			NavigateToPage(CurrentPage + 1);
			return true;
		}

		public bool NavigateToPrevPage()
		{
			if (HasPrevPage() == false)
				return false;
			NavigateToPage(CurrentPage + 1);
			return true;
		}

		private void NavigateToPage(int page)
		{
			url.SetQueryParam("skip", skip + PageSize);
			url.NavigateTo();
		}
	}
}