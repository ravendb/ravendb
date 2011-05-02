using Raven.Abstractions.Data;

namespace Raven.Studio.Features.Statistics
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
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

        public IEnumerable<Error> Errors
		{
			get { return server.Errors.Select( x => new Error(x)); }
		}

		public IServer Server
		{
			get { return server; }
		}
	}

    public class Error
    {
        private readonly ServerError inner;

        public Error(ServerError inner)
        {
            this.inner = inner;
        }

        public ServerError Inner { get { return inner; } }
    }
}