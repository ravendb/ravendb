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
		private readonly Func<DocumentsModel, Task> fetchDocuments;
		public BindableCollection<ViewableDocument> Documents { get; private set; }

		public DocumentsModel(Func<DocumentsModel, Task> fetchDocuments)
		{
			this.fetchDocuments = fetchDocuments;
			Documents = new BindableCollection<ViewableDocument>(new PrimaryKeyComparer<ViewableDocument>(document => document.Id));
		}
	
		protected override Task TimerTickedAsync()
		{
			return fetchDocuments(this);
		}

		public readonly PagerModel Pager = new PagerModel();

		public ICommand NextPage
		{
			get{ return new NavigateToNextPageCommand(Pager);}
		}

		public ICommand PreviousPage
		{
			get { return new NavigateToPrevPageCommand(Pager); }
		}
	}
}