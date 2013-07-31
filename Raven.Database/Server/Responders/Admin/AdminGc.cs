namespace Raven.Database.Server.Responders.Admin
{
	using Raven.Abstractions.Util;
	using Raven.Database.Server.Abstractions;

	public class AdminGc : AdminResponder
	{
		public override string[] SupportedVerbs
		{
			get { return new[] { "POST", "GET" }; }
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			RavenGC.CollectGarbage(false, () => Database.TransactionalStorage.ClearCaches());
		}
	}
}
