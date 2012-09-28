using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminGc : AdminResponder
	{
		public override string[] SupportedVerbs
		{
			get { return new[] {"POST", "GET"}; }
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			GC.Collect(2);
			GC.WaitForPendingFinalizers();
		}
	}
}