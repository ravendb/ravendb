using System;
using System.Threading.Tasks;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DocumentsModel : Model
	{
		public const double DefaultDocumentHeight = 66;
		public const double ExpandedDocumentHeight = 130;

		private readonly Func<DocumentsModel, Task> fetchDocuments;
		public BindableCollection<ViewableDocument> Documents { get; private set; }

		public bool SkipAutoRefresh { get; set; }

		public bool ShowEditControls { get; set; }
 
		public DocumentsModel(Func<DocumentsModel, Task> fetchDocuments)
		{
			this.fetchDocuments = fetchDocuments;
			Documents = new BindableCollection<ViewableDocument>(document => document.Id ?? document.DisplayId, new KeysComparer<ViewableDocument>(document => document.LastModified));
			Documents.CollectionChanged += (sender, args) => DetermineDocumentViewStyle();

			Pager = new PagerModel();
			Pager.Navigated += (sender, args) => ForceTimerTicked();

			ShowEditControls = true;
			DocumentHeight = 66;
		}

		private void DetermineDocumentViewStyle()
		{
			// assume that if the first document is a projection, then all the documents are projections
			var document = Documents.FirstOrDefault();
			if (document == null)
				return;

			if (document.CollectionType == "Projection")
			{
				DocumentHeight = Math.Max(DocumentHeight, ExpandedDocumentHeight);
			}
		}

		public override Task TimerTickedAsync()
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

		private double documentHeight;
		public double DocumentHeight
		{
			get { return documentHeight; }
			set
			{
				documentHeight = value;
				OnPropertyChanged();
				OnPropertyChanged("DocumentWidth");
			}
		}

		public double DocumentWidth
		{
			get
			{
				const double wideAspectRatio = 1.7;
				const double narrowAspectRatio = 0.707;
				const double aspectRatioSwitchoverHeight = 120;
				const double wideRatioMaxWidth = aspectRatioSwitchoverHeight*wideAspectRatio;
				const double narrowAspectRatioSwitchoverHeight = wideRatioMaxWidth/narrowAspectRatio;

				return DocumentHeight < aspectRatioSwitchoverHeight ? DocumentHeight * wideAspectRatio
				: DocumentHeight < narrowAspectRatioSwitchoverHeight ? wideRatioMaxWidth
				: DocumentHeight * narrowAspectRatio;
			}
		}

	}
}