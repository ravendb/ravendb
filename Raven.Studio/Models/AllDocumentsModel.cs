using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class AllDocumentsModel : ViewModel
	{
		static AllDocumentsModel()
		{
			Documents = new Observable<DocumentsModel>();
			Documents.Value = new DocumentsModel(GetFetchDocumentsMethod);
			Documents.Value.Pager.SetTotalResults(new Observable<long?>(ApplicationModel.Database.Value.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments));
		}

		public AllDocumentsModel()
		{
			ModelUrl = "/documents";
		}

		public static Observable<DocumentsModel> Documents { get; private set; }

		private static Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			return ApplicationModel.DatabaseCommands.GetDocumentsAsync(documentsModel.Pager.Skip, documentsModel.Pager.PageSize)
				.ContinueOnSuccess(docs => documentsModel.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}
	}
}