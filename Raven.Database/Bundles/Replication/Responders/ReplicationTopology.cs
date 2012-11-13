using System.Linq;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Bundles.Replication.Responders
{
	public class ReplicationTopology : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/replication/topology$"; }
		}
		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(new
			{
				ReplicationTask.Heartbeats,
				ReplicationTask.ReplicationFailureStats
			});
		}
	}
}