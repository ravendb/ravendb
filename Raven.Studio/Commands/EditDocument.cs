namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
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

		public void Execute(EditDocumentViewModel document)
		{
			events.Publish(new DatabaseScreenRequested(()=>document));
		}
	}
}