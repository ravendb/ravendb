using System.ComponentModel.Composition;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class ReplicationHeartbeat : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/replication/heartbeat"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var src = context.Request.QueryString["from"];

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			if(replicationTask == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "Cannot find replication task setup in the database"
				});
				return;
			}

			replicationTask.HandleHeartbeat(src);
		}
	}
}