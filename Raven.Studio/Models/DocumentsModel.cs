using System;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DocumentsModel : Model
	{
		private readonly Func<DocumentsModel, int, Task> fetchDocuments;
		private readonly int itemsPerPages;
		public BindableCollection<ViewableDocument> Documents { get; private set; }

		public DocumentsModel(Func<DocumentsModel,int, Task> fetchDocuments, string location, int itemsPerPages)
		{
			this.fetchDocuments = fetchDocuments;
			this.location = location;
			this.itemsPerPages = itemsPerPages;
			TotalPages = new Observable<long>();
			Documents = new BindableCollection<ViewableDocument>(new PrimaryKeyComparer<ViewableDocument>(document => document.Id));
		}
	
		protected override Task TimerTickedAsync()
		{
			return fetchDocuments(this, CurrentPage - 1);
		}

		public int CurrentPage
		{
			get { return UrlUtil.GetSkipCount() / itemsPerPages + 1; }
		}

		public ICommand NextPage
		{
			get{ return new IncreasePageCommand(location, itemsPerPages, TotalPages);}
		}

		public ICommand PreviousPage
		{
			get { return new DecreasePageCommand(location, itemsPerPages); }
		}

		private readonly string location;

		public Observable<long> TotalPages { get; set; }
	}
}