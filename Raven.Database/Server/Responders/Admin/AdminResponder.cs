using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public abstract class AdminResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/"+ GetType().Name.Replace("Admin", "") +"$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}


		protected bool EnsureSystemDatabase(IHttpContext context)
		{
			if (SystemDatabase == Database)
				return true;

			context.SetStatusToBadRequest();
			context.WriteJson(new
			{
				Error = "The request '" + context.GetRequestUrl() +"' can only be issued on the system database"
			});
			return false;
		}

		public override void Respond(Abstractions.IHttpContext context)
		{
			if (context.User.IsAdministrator() == false)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new
				{
					Error = "The operation '" + context.GetRequestUrl() +"' is only available to administrators"
				});
				return;
			}

			RespondToAdmin(context);
		}

		public abstract void RespondToAdmin(IHttpContext context);
	}
}