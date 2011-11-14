using System;
using System.Threading.Tasks;
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
			Documents = new BindableCollection<ViewableDocument>(document => document.Id, new KeysComparer<ViewableDocument>(document => document.LastModified));
		}

		protected override Task TimerTickedAsync()
		{
			return fetchDocuments(this);
		}

		private readonly PagerModel pager = new PagerModel();
		public PagerModel Pager
		{
			get { return pager; }
		}

		private string viewTitle;
		public string ViewTitle
		{
			get { return viewTitle ?? (viewTitle = "Documents"); }
			set { viewTitle = value; OnPropertyChanged(); }
		}
	}
}