using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminCompact : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

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
				SystemDatabase.TransactionalStorage.Compact(configuration));
		}

	}
}