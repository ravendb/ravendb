namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows;
    using Caliburn.Micro;
    using Client.Silverlight.Common;
    using Client.Silverlight.Data;
    using CommonViewModels;
    using Models;
    using Plugin;

    public class DocumentsScreenViewModel : Screen, IRavenScreen
    {
        private IList<DocumentViewModel> documents;
        private DocumentViewModel previewedDocument;
        private Visibility documentPreview;
        private bool isBusy;

        public DocumentsScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Browse";
            this.Database = database;
            this.DocumentPreview = Visibility.Collapsed;
        }

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

        public DocumentViewModel PreviewedDocument
        {
            get
            {
                return this.previewedDocument;
            }

            set
            {
                this.previewedDocument = value;
                
                if (this.previewedDocument != null)
                {
                    this.DocumentPreview = Visibility.Visible;
                }
                else
                {
                    this.DocumentPreview = Visibility.Collapsed;
                }

                NotifyOfPropertyChange(() => this.PreviewedDocument);
            }
        }

        public Visibility DocumentPreview
        {
            get
            {
                return this.documentPreview;
            }

            set
            {
                this.documentPreview = value;
                NotifyOfPropertyChange(() => this.DocumentPreview);
            }
        }

        public IList<DocumentViewModel> Documents
        {
            get
            {
                if (this.documents == null)
                {
                    this.IsBusy = true;
                    this.Database.Session.LoadMany<JsonDocument>(this.GetAll);
                }

                return this.documents;
            }

            set
            {
                this.documents = value;
                NotifyOfPropertyChange(() => this.Documents);
            }
        }

        public void GetAll(LoadResponse<IList<JsonDocument>> response)
        {
            IList<Document> result = response.Data.Select(jsonDocument => new Document(jsonDocument)).ToList();
            this.Documents = result.Select((document, index) => new DocumentViewModel(document, this)).ToList();
            this.IsBusy = false;
        }
    }
}