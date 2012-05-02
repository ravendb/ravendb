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

		private static WeakReference<Observable<DocumentsModelEnhanced>> documents;
        public static Observable<DocumentsModelEnhanced> Documents
		{
			get
			{
				if (documents == null || documents.IsAlive == false)
				{
                    documents = new WeakReference<Observable<DocumentsModelEnhanced>>(new Observable<DocumentsModelEnhanced>
                                                                                          {
                                                                                              Value = new DocumentsModelEnhanced(new DocumentsCollectionSource())
                                                                                                          {
                                                                                                              DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index)
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