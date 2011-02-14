namespace Raven.Studio.Commands
{
	using System;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Plugin;

	public class EditDocumentById
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public EditDocumentById(IEventAggregator events, IServer server)
		{
			this.events = events;
			this.server = server;
		}

		public void Execute(string documentId)
		{
			events.Publish(new WorkStarted());
			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetAsync(documentId)
				.ContinueOnSuccess( get =>
				               	{
				               		var doc = IoC.Get<DocumentViewModel>();
									doc.Initialize(get.Result);
									events.Publish(new OpenDocumentForEdit(doc));
									events.Publish(new WorkCompleted());
				               	});
		}
	}
}