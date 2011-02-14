namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Indexes;
	using Framework;
	using Messages;
	using Plugin;

	public class EditIndex
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public EditIndex(IEventAggregator events, IServer server)
		{
			this.events = events;
			this.server = server;
		}

		public void Execute(string indexName)
		{
			events.Publish(new WorkStarted());
			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetIndexAsync(indexName)
				.ContinueOnSuccess(get =>
					{
						events.Publish(new DatabaseScreenRequested(() => new EditIndexViewModel(get.Result, server)));
						events.Publish(new WorkCompleted());
					});
		}
	}
}