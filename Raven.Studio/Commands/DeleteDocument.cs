namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	public class DeleteDocument
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public DeleteDocument(IServer server, IEventAggregator events)
		{
			this.server = server;
			this.events = events;
		}

		public void Execute(string documentId)
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.DeleteDocumentAsync(documentId);
			}

			events.Publish(new StatisticsUpdateRequested());
			events.Publish(new DocumentDeleted(documentId));
		}
	}
}