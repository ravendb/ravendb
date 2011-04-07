namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Documents;
	using Framework.Extensions;
	using Messages;
	using Plugins;

	public class LoadDocument
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public LoadDocument(IEventAggregator events, IServer server)
		{
			this.events = events;
			this.server = server;
		}

		public void Execute(EditDocumentViewModel screen)
		{
			events.Publish(new WorkStarted("refreshing document"));
			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetAsync(screen.Id)
				.ContinueOnSuccess(get =>
				{
					events.Publish(new WorkCompleted("refreshing document"));
					screen.Initialize(get.Result);
					events.Publish(new NotificationRaised(screen.Id + " refreshed", NotificationLevel.Info));
				});
		}
	}
}