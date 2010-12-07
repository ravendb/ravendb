using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Net;
using Caliburn.Micro;
using Raven.Database;
using Raven.Management.Client.Silverlight.Common;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Dialogs;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.Models;
using Raven.ManagementStudio.UI.Silverlight.Plugins.Common;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    public class DocumentsScreenViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
    {
        private bool _isBusy;
        private bool _isDocumentPreviewed;
        private string _lastSearchDocumentId;

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
            get { return _isBusy; }
            set
            {
                _isBusy = value;
                NotifyOfPropertyChange(() => IsBusy);
            }
        }

        public IDatabase Database { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section { get { return SectionType.Documents; } }

        public bool IsDocumentPreviewed
        {
            get { return _isDocumentPreviewed && ActiveItem != null; }
            set
            {
                _isDocumentPreviewed = value;
                NotifyOfPropertyChange(() => IsDocumentPreviewed);
            }
        }

        public void GetAll(LoadResponse<IList<JsonDocument>> response)
        {
            IList<DocumentViewModel> result = response.Data.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), Database, this)).ToList();
            Items.AddRange(result);
            IsBusy = false;
        }

        public void ClosePreview()
        {
            IsDocumentPreviewed = false;
        }

        public void ShowDocument(string documentId)
        {
            _lastSearchDocumentId = documentId;

            if (!documentId.Equals("Document ID") && !documentId.Equals(string.Empty))
            {
                IsBusy = true;
                Database.Session.Load<JsonDocument>(documentId, GetDocument);
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
                                                                     _lastSearchDocumentId)));
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
            Database.Session.LoadMany<JsonDocument>(GetAll);
        }
    }
}
