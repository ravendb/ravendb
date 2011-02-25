namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
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

		public void Execute(DocumentViewModel document)
		{
			var editScreen = IoC.Get<EditDocumentViewModel>();
			editScreen.Initialize(document.JsonDocument);

			events.Publish(new DatabaseScreenRequested(() => editScreen));
		}
	}
}