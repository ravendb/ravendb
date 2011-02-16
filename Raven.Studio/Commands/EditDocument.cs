namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
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
			events.Publish(new DatabaseScreenRequested(()=>document));
		}
	}
}