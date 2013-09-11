using System.Linq;
using Nancy;
using Raven.Client;
using Raven.ClusterManager.Models;

namespace Raven.ClusterManager.Modules
{
	public class ReplicationModule : NancyModule
	{
		public ReplicationModule(IDocumentSession session)
			: base("/api/replication")
		{
			Get["/"] = parameters =>
			{
				var replicationDatabases = session.Query<DatabaseRecord>()
				                       .OrderBy(record => record.Id)
									   .Take(1024)
				                       .ToList();

				return replicationDatabases;
			};
		}
	}
}