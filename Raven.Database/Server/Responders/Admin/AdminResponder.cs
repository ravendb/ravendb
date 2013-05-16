using System.Linq;
using System.Security.Principal;
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

		protected virtual WindowsBuiltInRole[] AdditionalSupportedRoles
		{
			get
			{
				return new WindowsBuiltInRole[0];
			}
		}

		public override void Respond(IHttpContext context)
		{
			if (context.User.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode) == false &&
				context.User.IsAdministrator(Database) == false && SupportedByAnyAdditionalRoles(context) == false)
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

		private bool SupportedByAnyAdditionalRoles(IHttpContext context)
		{
			return AdditionalSupportedRoles.Any(role => context.User.IsInRole(server.SystemConfiguration.AnonymousUserAccessMode, role));
		}

		public abstract void RespondToAdmin(IHttpContext context);
	}
}