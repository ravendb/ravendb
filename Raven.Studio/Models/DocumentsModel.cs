using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DocumentsModel : Model
	{
		public BindableCollection<ViewableDocument> Documents { get; private set; }

		public bool SkipAutoRefresh { get; set; }
		public bool ShowEditControls { get; set; }

		public Func<DocumentsModel, Task> CustomFetchingOfDocuments { get; set; }

		public DocumentsModel()
		{
			Documents = new BindableCollection<ViewableDocument>(document => document.Id ?? document.DisplayId, new KeysComparer<ViewableDocument>(document => document.LastModified));
			Documents.CollectionChanged += (sender, args) => DetermineDocumentViewStyle();

			Pager = new PagerModel();
			Pager.PagerChanged += (sender, args) => ForceTimerTicked();

			ShowEditControls = true;
		}

		private void DetermineDocumentViewStyle()
		{
			// assume that if the first document is a projection, then all the documents are projections
			var document = Documents.FirstOrDefault();
			if (document == null)
				return;

			if (document.CollectionType == "Projection")
			{
				DocumentSize.Current.Height = Math.Max(DocumentSize.Current.Height, DocumentSize.ExpandedDocumentHeight);
			}
		}

		public override Task TimerTickedAsync()
		{
			if (SkipAutoRefresh && IsForced == false)
			{
				IsLoadingDocuments = false;
				return null;
			}

			if (IsForced)
				IsLoadingDocuments = true;

			var fetchingDocuments = CustomFetchingOfDocuments != null ? CustomFetchingOfDocuments(this) : DefaultFetchingOfDocuments();
			return fetchingDocuments
				.FinallyInTheUIThread(() => IsLoadingDocuments = false);
		}

		private Task DefaultFetchingOfDocuments()
		{
			return ApplicationModel.DatabaseCommands.GetDocumentsAsync(Pager.Skip, Pager.PageSize)
				.ContinueOnSuccess(docs =>
				{
					Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray());
					var documetsIds = new List<string>();
					foreach (var viewableDocument in Documents)
					{
						documetsIds.Add(viewableDocument.Id);

						viewableDocument.NeighborsIds = documetsIds;
					}
				});
		}

		public PagerModel Pager { get; private set; }

		private string header;
		public string Header
		{
			get { return header ?? (header = "Documents"); }
			set
			{
				header = value;
				OnPropertyChanged(() => Header);
			}
		}

		private bool isLoadingDocuments;
		public bool IsLoadingDocuments
		{
			get { return isLoadingDocuments; }
			set
			{
				isLoadingDocuments = value;
				OnPropertyChanged(() => IsLoadingDocuments);
			}
		}
	}
}