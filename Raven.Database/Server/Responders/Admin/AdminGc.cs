using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	using System.Runtime;

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

			CollectGarbage(Database, compactLoh: false);
		}

		public static void CollectGarbage(DocumentDatabase database, bool compactLoh)
		{
			if (compactLoh)
				GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
			database.TransactionalStorage.ClearCaches();
			GC.WaitForPendingFinalizers();
		}
	}
}