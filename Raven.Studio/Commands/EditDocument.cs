namespace Raven.Studio.Commands
{
    using System.Collections;
    using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
	using Features.Documents;
	using Messages;

	public class EditDocument
	{
		readonly IEventAggregator events;

		[ImportingConstructor]
		public EditDocument(IEventAggregator events)
		{
			this.events = events;
		}

        public bool CanExecute(object listOrViewModel)
        {
            if (listOrViewModel == null)
                return false;

            var list = listOrViewModel as IList;
            if (list != null)
            {
                return list.Count > 0;
            }

            var viewModel = listOrViewModel as DocumentViewModel;
            return viewModel != null;
        }

        public void Execute(object listOrViewModel) {
            DocumentViewModel document;

            var list = listOrViewModel as IList;
            if (list != null) {
                document = list.OfType<DocumentViewModel>()
                .FirstOrDefault();
            }
            else {
                document = listOrViewModel as DocumentViewModel;
            }

            if (document == null)
                return;

			var editScreen = IoC.Get<EditDocumentViewModel>();
			editScreen.Initialize(document.JsonDocument);

			events.Publish(new DatabaseScreenRequested(() => editScreen));
		}
	}
}