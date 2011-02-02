namespace Raven.Studio.Documents
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Net;
	using Caliburn.Micro;
	using Dialogs;
	using Framework;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Plugins.Common;
	using Raven.Database;

	public class DocumentsViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
	{
		bool isBusy;
		bool isDocumentPreviewed;
		string lastSearchDocumentId;
		readonly IDatabase database;

		public DocumentsViewModel(IDatabase database)
		{
			DisplayName = "Browse Documents";
			this.database = database;

			Documents = new BindablePagedQuery<JsonDocument>(database.Session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync);
			Documents.PageSize = 25;

			CompositionInitializer.SatisfyImports(this);
		}

		public BindablePagedQuery<JsonDocument> Documents { get; private set; }

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		[Import]
		public IWindowManager WindowManager { get; set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		public bool IsDocumentPreviewed
		{
			get { return isDocumentPreviewed && ActiveItem != null; }
			set
			{
				isDocumentPreviewed = value;
				NotifyOfPropertyChange(() => IsDocumentPreviewed);
			}
		}

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public void GetAll(IList<JsonDocument> response)
		{
			List<DocumentViewModel> result =
				response.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), database)).ToList();
			Items.AddRange(result);
			IsBusy = false;
		}

		public void ClosePreview()
		{
			IsDocumentPreviewed = false;
		}

		public void ShowDocument(string documentId)
		{
			lastSearchDocumentId = documentId;

			if (!documentId.Equals("Document ID") && !documentId.Equals(string.Empty))
			{
				IsBusy = true;
				throw new NotImplementedException();
				//Database.Session.Load<JsonDocument>(documentId, GetDocument);
			}
		}

		public void GetDocument(LoadResponse<JsonDocument> loadResponse)
		{
			IsBusy = false;
			if (loadResponse.IsSuccess)
			{
				NavigateTo(new Document(loadResponse.Data));
			}
			else if (loadResponse.StatusCode == HttpStatusCode.NotFound)
			{
				WindowManager.ShowDialog(new InformationDialogViewModel("Document not found",
				                                                        string.Format(
				                                                        	"Document with key {0} doesn't exist in database.",
				                                                        	lastSearchDocumentId)));
			}
		}

		void NavigateTo(Document document)
		{
			EventAggregator.Publish(
				new ReplaceActiveScreen(new DocumentViewModel(document, database)));
		}

		public void CreateDocument()
		{
			NavigateTo(new Document(new JsonDocument
			                        	{
			                        		DataAsJson = new JObject(),
			                        		Metadata = new JObject()
			                        	}));
		}

		protected override void OnActivate()
		{
			IsBusy = true;
			database.Session.Advanced.AsyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueWith(x => RefreshDocuments(x.Result.CountOfDocuments));
		}

		public void RefreshDocuments(long total)
		{
			Documents.GetTotalResults = () => total;
			Documents.LoadPage();
			IsBusy = false;
		}
	}

	public class LoadResponse<T>
	{
		public HttpStatusCode StatusCode;
		public bool IsSuccess { get; set; }

		public JsonDocument Data { get; set; }
	}
}