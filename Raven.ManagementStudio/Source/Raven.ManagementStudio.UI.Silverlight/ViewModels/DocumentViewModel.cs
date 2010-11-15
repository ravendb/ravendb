namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using Models;

    public class DocumentViewModel
    {
        private Document document;

        public DocumentViewModel(Models.Document document)
        {
            this.Document = document;
        }

        public Document Document
        {
            get { return this.document; }
            set { this.document = value; }
        }
    }
}