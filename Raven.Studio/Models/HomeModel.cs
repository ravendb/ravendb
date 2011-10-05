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
        private const int RecentDocumentsCountPerPage = 15;
        public Observable<DocumentsModel> RecentDocuments { get; private set; }

        public HomeModel(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            RecentDocuments = new Observable<DocumentsModel>
            {
                Value = new DocumentsModel(GetFetchDocumentsMethod, "/home", RecentDocumentsCountPerPage)
                {
                    TotalPages = new Observable<long>(database.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / RecentDocumentsCountPerPage + 1)
                }
            };
        }

        private Task GetFetchDocumentsMethod(BindableCollection<ViewableDocument> documents, int currentPage)
        {
            return asyncDatabaseCommands.GetDocumentsAsync(currentPage * RecentDocumentsCountPerPage, RecentDocumentsCountPerPage)
                .ContinueOnSuccess(docs => documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
        }

    }
}