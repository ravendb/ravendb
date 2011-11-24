using System;
using System.Threading.Tasks;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
    public enum DocumentViewStyle
    {
        Card,
        Details,
    }

	public class DocumentsModel : Model
	{
		private readonly Func<DocumentsModel, Task> fetchDocuments;
		public BindableCollection<ViewableDocument> Documents { get; private set; }

		public bool SkipAutoRefresh { get; set; }

        public bool ShowEditControls { get; set; }

        public Observable<DocumentViewStyle> ViewStyle { get; private set; }
 
		public DocumentsModel(Func<DocumentsModel, Task> fetchDocuments)
		{
			this.fetchDocuments = fetchDocuments;
			Documents = new BindableCollection<ViewableDocument>(document => document.Id ?? document.DisplayId, new KeysComparer<ViewableDocument>(document => document.LastModified));
		    Documents.Updated += delegate
		                             {
		                                 DetermineDocumentViewStyle();
		                             };

			Pager = new PagerModel();
			Pager.Navigated += (sender, args) => ForceTimerTicked();
			ForceTimerTicked();

		    ShowEditControls = true;
            ViewStyle = new Observable<DocumentViewStyle>();
		}

	    private void DetermineDocumentViewStyle()
	    {
            // assume that if the first document is a projection, then all the documents are projections
	        var document = Documents.FirstOrDefault();

            if (document == null)
            {
                return;
            }

	        ViewStyle.Value = document.CollectionType == "Projection" ? DocumentViewStyle.Details : DocumentViewStyle.Card;
	    }

	    protected override Task TimerTickedAsync()
		{
			if (SkipAutoRefresh && IsForced == false)
				return null;
			return fetchDocuments(this);
		}

		public PagerModel Pager { get; private set; }

		private string viewTitle;
		public string ViewTitle
		{
			get { return viewTitle ?? (viewTitle = "Documents"); }
			set { viewTitle = value; OnPropertyChanged(); }
		}
	}
}