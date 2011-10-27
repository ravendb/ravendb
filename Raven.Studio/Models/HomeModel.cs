using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class HomeModel : ViewModel
	{
		public Observable<DocumentsModel> RecentDocuments { get; private set; }

		public HomeModel()
		{
			ModelUrl = "/home";
			RecentDocuments = new Observable<DocumentsModel>();
		    Initialize();
		}

		private void Initialize()
		{
            if (Database.Value == null)
            {
                Database.RegisterOnce(Initialize);
                return;
            }

			var documents = new DocumentsModel(GetFetchDocumentsMethod);
			documents.Pager.PageSize = 15;
			documents.Pager.SetTotalPages(new Observable<long>(Database.Value.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / documents.Pager.PageSize + 1));
			RecentDocuments.Value = documents;
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documents)
		{
			return DatabaseCommands.GetDocumentsAsync(documents.Pager.Skip, documents.Pager.PageSize)
				.ContinueOnSuccess(docs => documents.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}
	}
}