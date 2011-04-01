namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Features.Indexes;
	using Framework.Extensions;
	using Messages;

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
						events.Publish(new DatabaseScreenRequested(() => new EditIndexViewModel(get.Result, server, events)));
						events.Publish(new WorkCompleted());
					});
		}
	}
}