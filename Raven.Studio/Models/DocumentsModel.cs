using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class DocumentsModel : Model
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        private readonly Func<BindableCollection<ViewableDocument>, int, Task> fetchDocuments;
        private readonly bool isRecentDocuments;
        public BindableCollection<ViewableDocument> Documents { get; private set; }

        public DocumentsModel(IAsyncDatabaseCommands asyncDatabaseCommands, Func<BindableCollection<ViewableDocument>, int, Task> fetchDocuments)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            this.fetchDocuments = fetchDocuments;
            this.isRecentDocuments = isRecentDocuments;
            Documents = new BindableCollection<ViewableDocument>(new PrimaryKeyComparer<ViewableDocument>(document => document.Id));
        }

        protected override Task TimerTickedAsync()
        {
            return fetchDocuments(Documents, CurrentPage);
        }

        private int currentPage;
        public int CurrentPage
        {
            get { return currentPage; }
            set
            {
                currentPage = value;
                OnPropertyChanged();
            }
        }
    }
}