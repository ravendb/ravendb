namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Framework;
	using Messages;
	using Plugin;

	public class SaveDocument
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public SaveDocument(IEventAggregator events, IServer server)
		{
			this.events = events;
			this.server = server;
		}

		public void Execute(DocumentViewModel document)
		{
			document.PrepareForSave();

			server.OpenSession().Advanced.AsyncDatabaseCommands
				.PutAsync(document.Id, null, document.JsonDocument.DataAsJson,null)
				.ContinueOnSuccess(put => events.Publish(new DocumentUpdated(document)));
			
		}
	}
}