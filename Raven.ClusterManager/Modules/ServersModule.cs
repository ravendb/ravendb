using System.Linq;
using Nancy;
using Raven.Client;
using Raven.ClusterManager.Models;

namespace Raven.ClusterManager.Modules
{
	public class ServersModule : NancyModule
	{
		public ServersModule(IDocumentSession session)
			: base("/api/servers")
		{
			Get[""] = parameters =>
			{
				var servers = session.Query<ServerRecord>()
					.OrderByDescending(record => record.IsOnline)
					.ThenByDescending(record => record.LastOnlineTime)
					.Take(1024)
					.ToList();

				return new ClusterStatistics
				{
					Servers = servers,
				};
			};

			Delete["/{id}"] = parameters =>
			{
				// session.Advanced.DocumentStore.DatabaseCommands.Delete();
				return null;
			};
		}
	}
}