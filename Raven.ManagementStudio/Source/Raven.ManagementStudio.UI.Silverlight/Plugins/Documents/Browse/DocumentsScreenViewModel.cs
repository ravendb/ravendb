using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Net;
using Caliburn.Micro;
using Raven.Database;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Dialogs;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.Models;
using Raven.ManagementStudio.UI.Silverlight.Plugins.Common;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
	using System;

	public class DocumentsScreenViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
    {
        private bool isBusy;
        private bool isDocumentPreviewed;
        private string lastSearchDocumentId;

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

        public IDatabase Database { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section { get { return SectionType.Documents; } }

        public bool IsDocumentPreviewed
        {
            get { return isDocumentPreviewed && ActiveItem != null; }
            set
            {
                isDocumentPreviewed = value;
                NotifyOfPropertyChange(() => IsDocumentPreviewed);
            }
        }

        public void GetAll(LoadResponse<IList<JsonDocument>> response)
        {
			throw new NotImplementedException();
			//IList<DocumentViewModel> result = response.Data.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), Database, this)).ToList();
			//Items.AddRange(result);
			//IsBusy = false;
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
                                                       string.Format("Document with key {0} doesn't exist in database.",
                                                                     lastSearchDocumentId)));
            }
        }

        private void NavigateTo(Document document)
        {
            EventAggregator.Publish(
                new ReplaceActiveScreen(new DocumentViewModel(document, Database, this)));
        }

        public void CreateDocument()
        {
            NavigateTo(new Document(new JsonDocument
                                        {
                                            DataAsJson = new Newtonsoft.Json.Linq.JObject(),
                                            Metadata = new Newtonsoft.Json.Linq.JObject()
                                        }));
        }

        protected override void OnInitialize()
        {
            base.OnInitialize();
            IsBusy = true;
			throw new NotImplementedException();
			//Database.Session.LoadMany<JsonDocument>(GetAll);
        }
    }

	public class LoadResponse<T>
	{
		public HttpStatusCode StatusCode;
		public bool IsSuccess { get; set; }

		public JsonDocument Data { get; set; }
	}
}
