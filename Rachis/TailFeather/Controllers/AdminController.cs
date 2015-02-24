using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Rachis.Transport;

namespace TailFeather.Controllers
{
	public class AdminController : TailFeatherController
	{
		[HttpGet]
		[Route("tailfeather/admin/flock")]
		public HttpResponseMessage Topology()
		{
			return Request.CreateResponse(HttpStatusCode.OK, new
			{
				RaftEngine.CurrentLeader,
				RaftEngine.PersistentState.CurrentTerm,
				RaftEngine.State,
				RaftEngine.CommitIndex,
				RaftEngine.CurrentTopology.AllVotingNodes,
				RaftEngine.CurrentTopology.PromotableNodes,
				RaftEngine.CurrentTopology.NonVotingNodes
			});
		}

		[HttpGet]
		[Route("tailfeather/admin/fly-with-us")]
		public async Task<HttpResponseMessage> Join([FromUri] string url, [FromUri] string name)
		{
			var uri = new Uri(url);
			name = name ?? uri.Host + (uri.IsDefaultPort ? "" : ":" + uri.Port);

			await RaftEngine.AddToClusterAsync(new NodeConnectionInfo
			{
				Name = name,
				Uri = uri
			});
			return new HttpResponseMessage(HttpStatusCode.Accepted);
		}

		[HttpGet]
		[Route("tailfeather/admin/fly-away")]
		public async Task<HttpResponseMessage> Leave([FromUri] string name)
		{
			await RaftEngine.RemoveFromClusterAsync(new NodeConnectionInfo
			{
				Name = name
			});
			return new HttpResponseMessage(HttpStatusCode.Accepted);
		}
	}
}