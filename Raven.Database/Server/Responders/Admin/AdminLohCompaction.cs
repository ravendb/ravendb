// -----------------------------------------------------------------------
//  <copyright file="AdminLohCompaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Server.Responders.Admin
{
	using Raven.Abstractions.Util;
	using Raven.Database.Server.Abstractions;

	public class AdminLohCompaction : AdminResponder
	{
		public override string[] SupportedVerbs
		{
			get { return new[] { "POST", "GET" }; }
		}

		public override string UrlPattern
		{
			get { return "^/admin/loh-compaction$"; }
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			RavenGC.CollectGarbage(true, () => Database.TransactionalStorage.ClearCaches());
		}
	}
}