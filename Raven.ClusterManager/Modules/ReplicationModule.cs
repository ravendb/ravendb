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
				                       .Where(record => record.IsReplicationEnabled)
				                       .OrderBy(record => record.Id)
				                       .ToList();

				return replicationDatabases;
			};
		}
	}
}