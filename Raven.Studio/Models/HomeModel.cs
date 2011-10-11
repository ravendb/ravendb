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
		private const int RecentDocumentsCountPerPage = 15;
		public Observable<DocumentsModel> RecentDocuments { get; private set; }

		public HomeModel()
		{
			RecentDocuments = new Observable<DocumentsModel>();
		}

		protected override void Initialize()
		{
			RecentDocuments.Value = new DocumentsModel(GetFetchDocumentsMethod, "/home", RecentDocumentsCountPerPage)
			{
				TotalPages = new Observable<long>(Database.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / RecentDocumentsCountPerPage + 1)
			};
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documents, int currentPage)
		{
			return DatabaseCommands.GetDocumentsAsync(currentPage * RecentDocumentsCountPerPage, RecentDocumentsCountPerPage)
				.ContinueOnSuccess(docs => documents.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}
	}
}