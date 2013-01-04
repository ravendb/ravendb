using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminIndexingStatus : AdminResponder
	{
		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}
		public override void RespondToAdmin(IHttpContext context)
		{
			context.WriteJson(new { IndexingStatus = Database.WorkContext.RunIndexing ? "Indexing" : "Paused" });
		}
	}
}
