using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminStopIndexing : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/stopindexing$"; }
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
					Error = "Only administrators can stop indexing"
				});
				return;
			}

			Database.StopBackgroundWorkers();
		}
	}
}