using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class AllDocumentsModel : ViewModel
	{
		static AllDocumentsModel()
		{
			SetTotalResults();
			ApplicationModel.Database.PropertyChanged += (sender, args) => SetTotalResults();
		}

		private static void SetTotalResults()
		{
			Documents.Value.Pager.SetTotalResults(new Observable<long?>(ApplicationModel.Database.Value.Statistics, v => ((DatabaseStatistics) v).CountOfDocuments));
		}

		public AllDocumentsModel()
		{
			ModelUrl = "/documents";
		}

		private static WeakReference<Observable<DocumentsModel>> documents;
		public static Observable<DocumentsModel> Documents
		{
			get
			{
				if (documents == null || documents.IsAlive == false)
					documents = new WeakReference<Observable<DocumentsModel>>(new Observable<DocumentsModel> { Value = new DocumentsModel() });
				return documents.Target;
			}
		}

		public override void LoadModelParameters(string parameters)
		{
			Documents.Value.Pager.SetSkip(new UrlParser(parameters));
		}

		public override Task TimerTickedAsync()
		{
			return Documents.Value.TimerTickedAsync();
		}
	}
}