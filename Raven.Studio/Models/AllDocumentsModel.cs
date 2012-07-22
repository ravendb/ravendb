using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class AllDocumentsModel : PageViewModel
	{
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
				{
					documents = new WeakReference<Observable<DocumentsModel>>(new Observable<DocumentsModel>
																						  {
																							  Value = CreateDocumentsModel()
																						  });
				}
				var target = documents.Target ?? Documents;
				return target;
			}
		}

		private static DocumentsModel CreateDocumentsModel()
		{
			var documentsModel = new DocumentsModel(new DocumentsCollectionSource())
									 {
										 DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
										 Context = "AllDocuments",
									 };

			documentsModel.SetChangesObservable(d => d.Changes().ForAllDocuments().Select(s => Unit.Default));

			return documentsModel;
		}

		public override void LoadModelParameters(string parameters)
		{
			Documents.Value.Documents.Refresh();
		}
	}
}