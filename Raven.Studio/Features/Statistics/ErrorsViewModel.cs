using Raven.Abstractions.Data;

namespace Raven.Studio.Features.Statistics
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Plugins;

    //NOTE: it would probably make more sense to remove IServer.Errors and rely on the message StatisticsUpdated
	[Export]
	public class ErrorsViewModel : Screen
	{
		readonly IServer server;

		[ImportingConstructor]
		public ErrorsViewModel(IServer server)
		{
			DisplayName = "Errors";
			this.server = server;
			server.CurrentDatabaseChanged += delegate { NotifyOfPropertyChange( ()=> Errors );};
		}

		public IEnumerable<ServerError> Errors
		{
			get { return server.Errors; }
		}

		public IServer Server
		{
			get { return server; }
		}
	}
}