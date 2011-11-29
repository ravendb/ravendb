using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class AllDocumentsModel : ViewModel
	{
		public AllDocumentsModel()
		{
			ModelUrl = "/documents";

			Documents = new Observable<DocumentsModel>();
			Documents.Value = new DocumentsModel(GetFetchDocumentsMethod);
			Documents.Value.Pager.SetTotalResults(new Observable<long?>(Database.Value.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments));
		}

		public Observable<DocumentsModel> Documents { get; private set; }

		private Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			return DatabaseCommands.GetDocumentsAsync(documentsModel.Pager.Skip, documentsModel.Pager.PageSize)
				.ContinueOnSuccess(docs => documentsModel.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}
	}
}