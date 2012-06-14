using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminStats : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/stats$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.User.IsAdministrator() == false)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new
				{
					Error = "Only administrators can look at the server stats"
				});
				return;
			}

			if(ResourceStore != DefaultResourceStore)
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