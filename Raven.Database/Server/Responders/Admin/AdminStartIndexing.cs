using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminStartIndexing : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			var concurrency = context.Request.QueryString["concurrency"];

			if(string.IsNullOrEmpty(concurrency)==false)
			{
				Database.Configuration.MaxNumberOfParallelIndexTasks = Math.Max(1, int.Parse(concurrency));
			}
			
			Database.SpinBackgroundWorkers();
		}
	}
}