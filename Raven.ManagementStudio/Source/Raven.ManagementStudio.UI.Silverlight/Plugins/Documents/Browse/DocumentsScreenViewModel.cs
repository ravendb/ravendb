namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Controls;
    using Caliburn.Micro;
    using Client.Silverlight.Common;
    using Client.Silverlight.Data;
    using CommonViewModels;
    using Models;
    using Plugin;

    public class DocumentsScreenViewModel : Screen, IRavenScreen
    {
        private IList<DocumentViewModel> documents;
        private bool isBusy;

        public DocumentsScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Browse";
            this.Database = database;
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

        public void DocumentsSelectionChanged(ListBox listBox)
        {
            foreach (var document in this.Documents)
            {
                document.IsSelected = false;
            }

            foreach (var document in listBox.SelectedItems.OfType<DocumentViewModel>())
            {
                document.IsSelected = true;
            }
        }

        public void GetAll(LoadResponse<IList<JsonDocument>> response)
        {
            IList<Document> result = response.Data.Select(jsonDocument => new Document(jsonDocument)).ToList();
            this.Documents = result.Select((document, index) => new DocumentViewModel(document, this)).ToList();
            this.IsBusy = false;
        }

        public IRavenScreen ParentRavenScreen { get; set; }
    }
}