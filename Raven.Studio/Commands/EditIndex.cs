using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Features.Indexes;
	using Framework.Extensions;
	using Messages;
	using Plugins;

	public class EditIndex
	{
		readonly IEventAggregator events;
		readonly IServer server;
		private readonly NavigationService navigationService;

		[ImportingConstructor]
		public EditIndex(IEventAggregator events, IServer server, NavigationService navigationService)
		{
			this.events = events;
			this.server = server;
			this.navigationService = navigationService;
		}

		public void Execute(string indexName)
		{
			events.Publish(new WorkStarted());
			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetIndexAsync(indexName)
				.ContinueOnSuccess(get =>
				{
					events.Publish(
						new DatabaseScreenRequested(() => new EditIndexViewModel(get.Result, server, events, navigationService)));
						events.Publish(new WorkCompleted());
					});
		}
	}
}