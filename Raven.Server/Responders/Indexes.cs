using System.Net;
using Raven.Server.Abstractions;

namespace Raven.Server.Responders
{
	public class Indexes : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "/indexes/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(Database.GetIndexes(context.GetStart(), context.GetPageSize()));
		}
	}
}