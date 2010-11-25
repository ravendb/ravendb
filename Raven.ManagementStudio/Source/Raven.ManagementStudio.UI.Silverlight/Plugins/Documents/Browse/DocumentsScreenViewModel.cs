namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using System.Net;
    using Caliburn.Micro;
    using Common;
    using Database;
    using Dialogs;
    using Management.Client.Silverlight.Common;
    using Messages;
    using Models;
    using Plugin;

    public class DocumentsScreenViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
    {
        private bool isBusy;
        private bool isDocumentPreviewed;
        private string lastSearchDocumentId;

        public DocumentsScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Browse";
            this.Database = database;
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        [Import]
        public IWindowManager WindowManager { get; set; }

        public bool IsBusy
        {
            get
            {
                return this.isBusy;
            }

            set
            {
                this.isBusy = value;
                NotifyOfPropertyChange(() => this.IsBusy);
            }
        }

        public IDatabase Database { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public bool IsDocumentPreviewed
        {
            get
            {
                return this.isDocumentPreviewed && this.ActiveItem != null;
            }

            set
            {
                this.isDocumentPreviewed = value;
                this.NotifyOfPropertyChange(() => this.IsDocumentPreviewed);
            }
        }

        public void GetAll(LoadResponse<IList<JsonDocument>> response)
        {
            IList<DocumentViewModel> result = response.Data.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), this.Database, this)).ToList();
            this.Items.AddRange(result);
            this.IsBusy = false;
        }

        public void ClosePreview()
        {
            this.IsDocumentPreviewed = false;
        }

        public void ShowDocument(string documentId)
        {
            this.lastSearchDocumentId = documentId;

            if (!documentId.Equals("Document ID") && !documentId.Equals(string.Empty))
            {
                this.IsBusy = true;
                this.Database.Session.Load<JsonDocument>(documentId, this.GetDocument);
            }
        }

        public void GetDocument(LoadResponse<JsonDocument> loadResponse)
        {
            this.IsBusy = false;
            if (loadResponse.IsSuccess)
            {
                this.EventAggregator.Publish(
                    new ReplaceActiveScreen(new DocumentViewModel(new Document(loadResponse.Data), this.Database, this)));
            }
            else if (loadResponse.StatusCode == HttpStatusCode.NotFound)
            {
                this.WindowManager.ShowDialog(new InformationDialogViewModel("Document not found",
                                                       string.Format("Document with key {0} doesn't exist in database.",
                                                                     this.lastSearchDocumentId)));
            }
            else
            {
                
            }
        }

        protected override void OnInitialize()
        {
            base.OnInitialize();
            this.IsBusy = true;
            this.Database.Session.LoadMany<JsonDocument>(this.GetAll);
        }
    }
}