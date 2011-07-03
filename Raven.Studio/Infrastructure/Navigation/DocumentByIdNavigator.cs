using System.Collections.Generic;
using System.ComponentModel.Composition;
using Caliburn.Micro;
using Raven.Studio.Commands;
using Raven.Studio.Features.Database;
using Raven.Studio.Messages;
using Raven.Studio.Plugins;

namespace Raven.Studio.Infrastructure.Navigation
{
	[ExportMetadata("Url", @"(?<database>\w+)/docs/(?<id>.*)")]
	[Export(typeof(INavigator))]
	public class DocumentByIdNavigator : INavigator
	{
		private readonly IServer server;
		private readonly IEventAggregator events;
		private readonly EditDocumentById editDocumentById;

		[ImportingConstructor]
		public DocumentByIdNavigator(IServer server, IEventAggregator events, EditDocumentById editDocumentById)
		{
			this.server = server;
			this.events = events;
			this.editDocumentById = editDocumentById;
		}

		public void Navigate(Dictionary<string, string> parameters)
		{
			var task = string.Format("Navigating to {0}", GetType().Name);
			events.Publish(new WorkStarted(task));

			var database = parameters["database"];
			if (server.CurrentDatabase != database)
				server.OpenDatabase(database, () => events.Publish(new DisplayCurrentDatabaseRequested()));
			
			editDocumentById.Execute(parameters["id"]);

			events.Publish(new WorkCompleted(task));
		}
	}
}