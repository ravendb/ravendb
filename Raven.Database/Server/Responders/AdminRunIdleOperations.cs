using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminRunIdleOperations : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/runidleoperations$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.User.IsAdministrator() == false)
			{
				context.SetStatusToUnauthorized();
				context.WriteJson(new
				{
					Error = "Only administrators can do that"
				});
				return;
			}

			Database.RunIdleOperations();
		}

	}
}
