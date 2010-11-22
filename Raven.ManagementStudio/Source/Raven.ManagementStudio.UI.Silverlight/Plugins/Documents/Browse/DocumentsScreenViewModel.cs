namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
    using CommonViewModels;
    using Database;
    using Management.Client.Silverlight.Common;
    using Messages;
    using Models;
    using Plugin;

    public class DocumentsScreenViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen
    {
        private bool isBusy;
        private bool isDocumentPreviewed;

        public DocumentsScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Browse";
            this.Database = database;
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

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
            if (!documentId.Equals("Document ID") && !documentId.Equals(string.Empty))
            {
                this.IsBusy = true;
                this.Database.Session.Load<JsonDocument>(documentId, this.GetDocument);
            }
        }

        public void GetDocument(LoadResponse<JsonDocument> jsonDocument)
        {
            this.IsBusy = false;
            if (jsonDocument.IsSuccess)
            {
                this.EventAggregator.Publish(
                    new ReplaceActiveScreen(new DocumentViewModel(new Document(jsonDocument.Data), this.Database, this)));
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