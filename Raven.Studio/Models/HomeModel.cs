using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class HomeModel : Model
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        public Observable<DocumentsModel> RecentDocuments { get; private set; }

        public HomeModel(IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            RecentDocuments = new Observable<DocumentsModel> {Value = new DocumentsModel(asyncDatabaseCommands, GetFetchDocumentsMethod())};
        }

        private Func<BindableCollection<ViewableDocument>, int, Task> GetFetchDocumentsMethod()
        {
            return (documents, currentPage) => asyncDatabaseCommands.GetDocumentsAsync(0, 15)
                .ContinueOnSuccess(docs => documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
        }

    }
}