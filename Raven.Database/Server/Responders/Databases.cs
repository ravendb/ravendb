using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Databases : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/databases/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					context.WriteJson(Database.GetDocumentsWithIdStartingWith("Raven/Databases/", context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize)));
					break;
			}
		}
	}
}