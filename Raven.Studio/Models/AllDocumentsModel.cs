using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class AllDocumentsModel : ViewModel
	{
		static AllDocumentsModel()
		{
			Documents = new Observable<DocumentsModel>();
			Documents.Value = new DocumentsModel();
			Documents.Value.Pager.SetTotalResults(new Observable<long?>(ApplicationModel.Database.Value.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments));
		}

		public AllDocumentsModel()
		{
			ModelUrl = "/documents";
		}

		public static Observable<DocumentsModel> Documents { get; private set; }
	}
}