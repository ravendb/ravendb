using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Statistics : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/stats$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(Database.Statistics);
		}
	}
}
