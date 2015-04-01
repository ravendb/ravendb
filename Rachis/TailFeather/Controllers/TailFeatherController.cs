using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using Rachis;
using Rachis.Utils;
using TailFeather.Storage;

namespace TailFeather.Controllers
{
	public abstract class TailFeatherController : ApiController
	{
		public KeyValueStateMachine StateMachine { get; private set; }
		public RaftEngine RaftEngine { get; private set; }

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			RaftEngine = (RaftEngine)controllerContext.Configuration.Properties[typeof(RaftEngine)];
			StateMachine = (KeyValueStateMachine)RaftEngine.StateMachine;
			try
			{
				return await base.ExecuteAsync(controllerContext, cancellationToken);
			}
			catch (NotLeadingException)
			{
				var currentLeader = RaftEngine.CurrentLeader;
				if (currentLeader == null)
				{
					return Request.CreateErrorResponse(HttpStatusCode.PreconditionFailed, "No current leader, try again later");
				}
				var leaderNode = RaftEngine.CurrentTopology.GetNodeByName(currentLeader);
				if (leaderNode == null)
				{
					return Request.CreateErrorResponse(HttpStatusCode.PreconditionFailed, "Current leader " + currentLeader + " is not found in the topology. This should not happen.");
				}
				return new HttpResponseMessage(HttpStatusCode.Redirect)
				{
					Headers =
					{
						Location = leaderNode.Uri
					}
				};
			}
		}
	}
}