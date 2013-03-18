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
				var statistics = new ClusterStatistics();
				statistics.Servers = session.Query<ServerRecord>()
					.OrderBy(record => record.Id)
					.Take(1024)
					.ToList();

				statistics.Credentials = session.Query<ServerCredentials>()
					.OrderByDescending(credentials => credentials.Id)
					.Take(1024)
					.ToList();


				return statistics;
			};

			Delete["/{id}"] = parameters =>
			{
				var id = (string)parameters.id;
				session.Advanced.DocumentStore.DatabaseCommands.Delete(id, null);
				return true;
			};
		}
	}
}