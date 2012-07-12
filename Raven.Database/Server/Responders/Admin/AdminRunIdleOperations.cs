using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminRunIdleOperations : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			Database.RunIdleOperations();
		}

	}
}
