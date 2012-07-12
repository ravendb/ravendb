using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminStats : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			if(Database != SystemDatabase)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Admin stats can only be had from the root database"
				});
				return;
			}

			context.WriteJson(server.Statistics);

		}
	}
}