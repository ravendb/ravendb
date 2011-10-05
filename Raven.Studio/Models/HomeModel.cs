using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class HomeModel : Model
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        public Observable<DocumentsModel> RecentDocuments { get; private set; }

        public HomeModel(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            RecentDocuments = new Observable<DocumentsModel>
            {
                Value = new DocumentsModel(GetFetchDocumentsMethod, "/Home", 15)
                {
                    TotalPages = new Observable<long>(database.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / 15)
                }
            };
        }

        private Task GetFetchDocumentsMethod(BindableCollection<ViewableDocument> documents, int currentPage)
        {
            return asyncDatabaseCommands.GetDocumentsAsync(GetSkipCount(), 15)
                .ContinueOnSuccess(docs => documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
        }

    }
}