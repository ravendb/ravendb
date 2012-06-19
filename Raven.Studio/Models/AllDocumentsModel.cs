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
                                                                                              Value = new DocumentsModel(new DocumentsCollectionSource())
                                                                                                          {
                                                                                                              DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
                                                                                                              Context = "AllDocuments",
                                                                                                          }
                                                                                          });
				}
				var target = documents.Target ?? Documents;
				return target;
			}
		}

		public override void LoadModelParameters(string parameters)
		{
		}

		public override Task TimerTickedAsync()
		{
			return Documents.Value.TimerTickedAsync();
		}
	}
}