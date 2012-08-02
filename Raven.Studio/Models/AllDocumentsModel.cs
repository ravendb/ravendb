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
	    private DocumentsModel documents;

	    public AllDocumentsModel()
		{
			ModelUrl = "/documents";
		}

		public DocumentsModel Documents
		{
			get
			{
				if (documents == null )
				{
				    documents = CreateDocumentsModel();
				}
				return documents;
			}
		}

		private static DocumentsModel CreateDocumentsModel()
		{
			var documentsModel = new DocumentsModel(new DocumentsCollectionSource())
									 {
										 DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
										 Context = "AllDocuments",
									 };

			documentsModel.SetChangesObservable(d => d.DocumentChanges.Select(s => Unit.Default));

			return documentsModel;
		}
	}
}