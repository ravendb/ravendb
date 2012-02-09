using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class AdminStartIndexing : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/startindexing$"; }
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
					Error = "Only administrators can start indexing"
				});
				return;
			}

			var concurrency = context.Request.QueryString["concurrency"];

			if(string.IsNullOrEmpty(concurrency)==false)
			{
				Database.Configuration.MaxNumberOfParallelIndexTasks = Math.Max(1, int.Parse(concurrency));
			}
			
			Database.SpinBackgroundWorkers();
		}
	}
}