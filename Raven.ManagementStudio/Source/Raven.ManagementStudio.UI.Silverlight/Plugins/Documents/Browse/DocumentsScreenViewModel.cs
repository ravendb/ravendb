namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using System.Collections.Generic;
    using System.Linq;
    using Caliburn.Micro;
    using CommonViewModels;
    using Database;
    using Management.Client.Silverlight.Common;
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
            IList<DocumentViewModel> result = response.Data.Select(jsonDocument => new DocumentViewModel(new Document(jsonDocument), this)).ToList();
            this.Items.AddRange(result);
            this.IsBusy = false;
        }

        public void ClosePreview()
        {
            this.IsDocumentPreviewed = false;
        }

        protected override void OnInitialize()
        {
            base.OnInitialize();
            this.IsBusy = true;
            this.Database.Session.LoadMany<JsonDocument>(this.GetAll);
        }
    }
}