namespace Raven.Studio.Plugins.Documents.Browse
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Net;
	using Caliburn.Micro;
	using Common;
	using Dialogs;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;

	public class DocumentsScreenViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
	{
		bool isBusy;
		bool isDocumentPreviewed;
		string lastSearchDocumentId;

		public DocumentsScreenViewModel(IDatabase database)
		{
			DisplayName = "Browse Documents";
			Database = database;
			CompositionInitializer.SatisfyImports(this);
		}

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

		public IDatabase Database { get; private set; }

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
				response.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), Database)).ToList();
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
				new ReplaceActiveScreen(new DocumentViewModel(document, Database)));
		}

		public void CreateDocument()
		{
			NavigateTo(new Document(new JsonDocument
			                        	{
			                        		DataAsJson = new JObject(),
			                        		Metadata = new JObject()
			                        	}));
		}

		protected override void OnInitialize()
		{
			base.OnInitialize();

			IsBusy = true;

			//Database.Session.Advanced.AsyncDatabaseCommands
			//    .QueryAsync(string.Empty,new IndexQuery(){}, new string[]{})
			//    .ContinueWith(x => GetAll(x.Result));
			throw new NotImplementedException();
		}
	}

	public class LoadResponse<T>
	{
		public HttpStatusCode StatusCode;
		public bool IsSuccess { get; set; }

		public JsonDocument Data { get; set; }
	}
}