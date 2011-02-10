namespace Raven.Studio.Documents
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Net;
	using Caliburn.Micro;
	using Client;
	using Dialogs;
	using Framework;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;

	[Export]
	public class BrowseDocumentsViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
	{
		bool isBusy;
		bool isDocumentPreviewed;
		string lastSearchDocumentId;
		readonly IServer server;
		readonly IWindowManager windowManager;
		readonly IEventAggregator events;

		[ImportingConstructor]
		public BrowseDocumentsViewModel(IServer server, IWindowManager windowManager, IEventAggregator events)
		{
			DisplayName = "Documents";
			this.server = server;
			this.windowManager = windowManager;
			this.events = events;
		}

		public BindablePagedQuery<JsonDocument> Documents { get; private set; }

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
				response.Select(jsonDocument => new DocumentViewModel(jsonDocument, server)).ToList();
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
				NavigateTo(loadResponse.Data);
			}
			else if (loadResponse.StatusCode == HttpStatusCode.NotFound)
			{
				windowManager.ShowDialog(new InformationDialogViewModel("Document not found",
				                                                        string.Format(
				                                                        	"Document with key {0} doesn't exist in database.",
				                                                        	lastSearchDocumentId)));
			}
		}

		void NavigateTo(JsonDocument document)
		{
			events.Publish(
				new ReplaceActiveScreen(new DocumentViewModel(document, server)));
		}

		public void CreateDocument()
		{
			NavigateTo(new JsonDocument
			                        	{
			                        		DataAsJson = new JObject(),
			                        		Metadata = new JObject()
			                        	});
		}

		protected override void OnActivate()
		{
			IsBusy = true;

			using(var session = server.OpenSession())
			{
				Documents = new BindablePagedQuery<JsonDocument>(session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync);
				Documents.PageSize = 25;

				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x => RefreshDocuments(x.Result.CountOfDocuments));
			}
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