using System.Reactive;
using System.Reactive.Linq;
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
		    ApplicationModel.Current.Server.Value.RawUrl = "databases/" +
		                                                   ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name +
		                                                   "/docs";
		}

		public DocumentsModel Documents
		{
			get { return documents ?? (documents = CreateDocumentsModel()); }
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