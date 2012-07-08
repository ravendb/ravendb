using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminCompact : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/compact$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[]{"POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.User.IsAdministrator() == false)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new
				{
					Error = "Only administrators can initiate a database compact procedure"
				});
				return;
			}

			if(DefaultResourceStore != ResourceStore)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Compact request can only be issued from the system database"
				});
				return;
			}

			var db = context.Request.QueryString["database"];
			if(string.IsNullOrWhiteSpace(db))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Compact request requires a valid database parameter"
				});
				return;
			}

			var configuration = server.CreateTenantConfiguration(db);
			if (configuration == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "No database named: " + db
				});
				return;
			}

			server.LockDatabase(db, () => 
				DefaultResourceStore.TransactionalStorage.Compact(configuration));
		}
	}
}