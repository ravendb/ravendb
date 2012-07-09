using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminStopIndexing : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			Database.StopBackgroundWorkers();
		}
	}
}